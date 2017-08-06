package net.gfxmonk.sequentialstate

import monix.execution.misc.NonFatal

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Try,Success,Failure}

trait EnqueueableTask {
	// Simplification for the scheduler, which doesn't
	// care about the type parameter in UnitOfWork
	def enqueuedAsync(): Unit
	def run(): Unit
}

trait UnitOfWork[A] extends EnqueueableTask {
	protected val fn: Function0[A]

	def enqueuedAsync(): Unit
	protected def reportSuccess(result: A): Unit
	protected def reportFailure(error: Throwable): Unit

	final def run() = {
		try {
			reportSuccess(fn())
		} catch {
			case e:Throwable => {
				if (NonFatal(e)) {
					reportFailure(e)
				} else {
					throw e
				}
			}
		}
	}
}

object UnitOfWork {
	trait HasExecutionContext {
		protected val ec: ExecutionContext
	}

	trait IgnoresResult[A] {
		final def reportSuccess(result: A): Unit = ()
		final def reportFailure(err: Throwable): Unit = ()
	}

	trait HasEnqueuePromise[A] {
		@volatile var _enqueuedPromise: Promise[A] = null
		def enqueuedPromise: Promise[A] = {
			if (_enqueuedPromise == null) {
				_enqueuedPromise = Promise()
			}
			_enqueuedPromise
		}
	}

	trait HasResultPromise[A] {
		val resultPromise: Promise[A] = Promise[A]()
	}

	trait HasSyncResult[A] { self: HasResultPromise[A] =>
		final def reportSuccess(result: A): Unit = {
			resultPromise.success(result)
		}

		final def reportFailure(error: Throwable): Unit = {
			resultPromise.failure(error)
		}
	}

	trait HasStagedResult[A] { self: HasResultPromise[A] with HasExecutionContext =>
		final def reportSuccess(result: Future[A]): Unit = {
			result.onComplete(resultPromise.complete)(ec)
		}

		final def reportFailure(error: Throwable): Unit = {
			// Make sure all failures happen as results, not enqueue
			reportSuccess(Future.failed(error))
		}
	}

	case class Full[A](fn: Function0[A])
		extends UnitOfWork[A]
		with HasEnqueuePromise[Future[A]]
		with HasResultPromise[A]
    with HasSyncResult[A]
	{
		final def enqueuedAsync() {
			enqueuedPromise.success(resultPromise.future)
		}
	}

	case class FullStaged[A](fn: Function0[StagedFuture[A]])(implicit ec: ExecutionContext)
		extends UnitOfWork[StagedFuture[A]]
			with HasEnqueuePromise[Future[A]]
			with HasResultPromise[A]
	{
		// ignore enqueues; we wait until the outer result future has been resolved
		final def enqueuedAsync(): Unit = ()

		final def reportSuccess(result: StagedFuture[A]): Unit = {
			result.onAccept { future =>
				enqueuedPromise.success(resultPromise.future)
				future.onComplete(resultPromise.complete)
			}
		}

		final def reportFailure(error: Throwable): Unit = {
			// Make sure all failures happen as results, not enqueue errors
			// (unlikely, so we don't care if it's ineffieicnt)
			reportSuccess(StagedFuture.accepted(Future.failed(error)))
		}
	}

	case class EnqueueOnly[A](fn: Function0[A])
		extends UnitOfWork[A]
		with HasEnqueuePromise[Unit]
		with IgnoresResult[A]
	{
		final def enqueuedAsync() {
			enqueuedPromise.success(())
		}
	}

	case class EnqueueOnlyStaged[A](fn: Function0[StagedFuture[A]])(implicit ec: ExecutionContext)
		extends UnitOfWork[StagedFuture[A]]
		with HasEnqueuePromise[Unit]
	{
		// ignore enqueues; we wait until the outer result future has been resolved
		final def enqueuedAsync(): Unit = ()

		final def reportSuccess(send: StagedFuture[A]): Unit = {
			send.onAccept((result:Future[A]) => enqueuedPromise.success(()))
		}

		final def reportFailure(error: Throwable): Unit = {
			enqueuedPromise.success(())
		}
	}

	case class ReturnOnly[A](fn: Function0[A])
		extends UnitOfWork[A] with HasResultPromise[A] with HasSyncResult[A]
	{
		final def enqueuedAsync(): Unit = ()
	}

	case class ReturnOnlyStaged[A](fn: Function0[Future[A]])(implicit val ec: ExecutionContext)
		extends UnitOfWork[Future[A]]
		with HasResultPromise[A]
		with HasExecutionContext
		with HasStagedResult[A]
	{
		final def enqueuedAsync(): Unit = ()
	}
}
