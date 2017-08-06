package net.gfxmonk.sequentialstate

import monix.execution.misc.NonFatal

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Try,Success,Failure}

trait EnqueueableTask {
	// Simplification for the scheduler, which doesn't
	// care about the type parameter in UnitOfWork
	def enqueuedAsync(): Unit
	def run(): Option[StagedFuture[_]]
}

trait UnitOfWork[A] extends EnqueueableTask {
	protected val fn: Function0[A]

	def enqueuedAsync(): Unit
	protected def reportSuccess(result: A): Option[StagedFuture[_]]
	protected def reportFailure(error: Throwable): Option[StagedFuture[_]]

	final def run(): Option[StagedFuture[_]] = {
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
		final def reportSuccess(result: A): Option[StagedFuture[_]] = None
		final def reportFailure(err: Throwable): Option[StagedFuture[_]] = None
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
		final def reportSuccess(result: A): Option[StagedFuture[_]] = {
			resultPromise.success(result)
			None
		}

		final def reportFailure(error: Throwable): Option[StagedFuture[_]] = {
			resultPromise.failure(error)
			None
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

		final def reportSuccess(result: StagedFuture[A]): Option[StagedFuture[_]] = {
			result.onAccept { future =>
				enqueuedPromise.success(resultPromise.future)
				future.onComplete(resultPromise.complete)
			}
			Some(result)
		}

		final def reportFailure(error: Throwable): Option[StagedFuture[_]] = {
			// Make sure all failures happen as results, not enqueue errors
			// (unlikely, so we don't care if it's ineffieicnt)
			reportSuccess(StagedFuture.accepted(Future.failed(error)))
			None
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

		final def reportSuccess(send: StagedFuture[A]): Option[StagedFuture[_]] = {
			send.onAccept((result:Future[A]) => enqueuedPromise.success(()))
			Some(send)
		}

		final def reportFailure(error: Throwable): Option[StagedFuture[_]] = {
			enqueuedPromise.success(())
			None
		}
	}

	case class ReturnOnly[A](fn: Function0[A])
		extends UnitOfWork[A] with HasResultPromise[A] with HasSyncResult[A]
	{
		final def enqueuedAsync(): Unit = ()
	}

	case class ReturnOnlyStaged[A](fn: Function0[StagedFuture[A]])(implicit val ec: ExecutionContext)
		extends UnitOfWork[StagedFuture[A]]
		with HasResultPromise[A]
		with HasExecutionContext
	{
		final def enqueuedAsync(): Unit = ()

		final def reportSuccess(result: StagedFuture[A]): Option[StagedFuture[_]] = {
			result.onComplete(resultPromise.complete)(ec)
			Some(result)
		}

		final def reportFailure(error: Throwable): Option[StagedFuture[_]] = {
			// Make sure all failures happen as results, not enqueue
			resultPromise.failure(error)
			None
		}
	}
}
