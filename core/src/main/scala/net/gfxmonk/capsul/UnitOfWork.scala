package net.gfxmonk.capsul

import net.gfxmonk.capsul.UnitOfWork.HasEnqueuePromise

import scala.util.control.NonFatal
import scala.concurrent.{ExecutionContext, Future, Promise}

trait EnqueueableTask {
	// Simplification for the scheduler, which doesn't
	// care about the type parameter in UnitOfWork
	def enqueuedAsync(): Unit
	def run(): Option[Future[_]]
}

trait AsyncTask {
	def spawn(onComplete: Function1[Any,Unit]): Boolean
	def enqueuedAsync(): Unit
}

trait UnitOfWork[A] extends EnqueueableTask {
	protected val fn: Function0[A]

	def enqueuedAsync(): Unit
	protected def reportSuccess(result: A): Option[Future[_]]
	protected def reportFailure(error: Throwable): Option[Future[_]]

	final def run(): Option[Future[_]] = {
		try {
			reportSuccess(fn())
		} catch {
			case NonFatal(e) => {
				reportFailure(e)
			}
		}
	}
}

object UnitOfWork {
	val noop: EnqueueableTask = new EnqueueableTask {
		override def enqueuedAsync() = ()
		override def run() = None
	}
	trait HasExecutionContext {
		protected val ec: ExecutionContext
	}

	trait IgnoresSuccess[A] {
		final def reportSuccess(result: A): Option[Future[_]] = None
	}

	trait IgnoresFailure {
		final def reportFailure(error: Throwable): Option[Future[_]] = {
			Console.err.println(s"Uncaught error in enqueued task: $error")
			error.printStackTrace()
			None
		}
	}

	trait HasEnqueuePromise[A] {
		var enqueuedPromise = Promise[A]()
	}

	trait HasResultPromise[A] {
		val resultPromise: Promise[A] = Promise[A]()
		final def reportFailure(error: Throwable): Option[Future[_]] = {
			resultPromise.failure(error)
			None
		}
	}

	trait HasSyncResult[A] { self: HasResultPromise[A] =>
		final def reportSuccess(result: A): Option[Future[_]] = {
			resultPromise.success(result)
			None
		}
	}

	trait HasEnqueueAndResultPromise[A] { self: HasEnqueuePromise[Future[A]] with HasResultPromise[A] =>
		final def enqueuedAsync() {
			enqueuedPromise.success(resultPromise.future)
		}
	}

	case class Full[A](fn: Function0[A])
		extends UnitOfWork[A]
		with HasEnqueuePromise[Future[A]]
		with HasResultPromise[A]
		with HasSyncResult[A]
		with HasEnqueueAndResultPromise[A]
	{
	}

	trait HasStagedResult[A] { self: UnitOfWork[StagedFuture[A]] with HasExecutionContext with HasResultPromise[A] =>
		final def reportSuccess(result: StagedFuture[A]): Option[Future[_]] = {
			result.onComplete(resultPromise.complete)(ec)
			Some(result.accepted)
		}
	}

	trait HasFutureResult[A] { self: UnitOfWork[Future[A]] with HasExecutionContext with HasResultPromise[A] =>
		final def reportSuccess(result: Future[A]): Option[Future[_]] = {
			result.onComplete(resultPromise.complete)(ec)
			Some(result)
		}

		final def reportSuccessAsync(result: Future[A]): Future[_] = {
			result.onComplete(resultPromise.complete)(ec)
			result
		}
	}

	case class FullStaged[A](fn: Function0[StagedFuture[A]])(implicit protected val ec: ExecutionContext)
		extends UnitOfWork[StagedFuture[A]]
			with HasExecutionContext
			with HasEnqueuePromise[Future[A]]
			with HasResultPromise[A]
			with HasEnqueueAndResultPromise[A]
			with HasStagedResult[A]
	{
	}

	case class FullAsync[A](fn: Function0[Future[A]])(implicit protected val ec: ExecutionContext)
		extends UnitOfWork[Future[A]]
			with HasExecutionContext
			with HasEnqueuePromise[Future[A]]
			with HasResultPromise[A]
			with HasEnqueueAndResultPromise[A]
			with HasFutureResult[A]

	trait IsEnqueueOnly extends IgnoresFailure { self: HasEnqueuePromise[Unit] =>
		final def enqueuedAsync() {
			enqueuedPromise.success(())
		}
	}

	case class EnqueueOnly[A](fn: Function0[A])
		extends UnitOfWork[A]
		with HasEnqueuePromise[Unit]
		with IsEnqueueOnly
		with IgnoresSuccess[A]
	{
	}

	case class EnqueueOnlyStaged[A](fn: Function0[StagedFuture[A]])(implicit ec: ExecutionContext)
		extends UnitOfWork[StagedFuture[A]]
		with HasEnqueuePromise[Unit]
		with IsEnqueueOnly
	{
		final def reportSuccess(send: StagedFuture[A]): Option[Future[_]] = {
			Some(send.accepted)
		}
	}

	case class EnqueueOnlyAsync[A](fn: Function0[Future[A]])
		extends UnitOfWork[Future[A]]
		with HasEnqueuePromise[Unit]
		with IsEnqueueOnly {
		override protected def reportSuccess(result: Future[A]): Option[Future[_]] = ???
	}

	case class BackpressureFullAsync[A](fn: Function0[Future[A]])(implicit ec: ExecutionContext)
		extends AsyncTask
		with HasEnqueuePromise[Future[A]]
		with HasResultPromise[A]
		with HasEnqueueAndResultPromise[A] {

		override def spawn(onComplete: Function1[Any,Unit]): Boolean = {
			val f = fn()
			val done = f.isCompleted
			if (!done) {
				f.onComplete(onComplete)
			}
			f.onComplete(resultPromise.complete)
			done
		}
	}

	case class BackpressureFull[A](fn: Function0[A])
		extends AsyncTask
			with HasEnqueuePromise[Future[A]]
			with HasResultPromise[A]
			with HasEnqueueAndResultPromise[A] {

		override def spawn(onComplete: Function1[Any,Unit]): Boolean = {
			resultPromise.success(fn())
			true
		}
	}

	case class BackpressureEnqueueOnlyAsync[A](fn: Function0[Future[A]])
		(implicit ec: ExecutionContext)
		extends AsyncTask with HasEnqueuePromise[Unit] with IsEnqueueOnly {

		override def spawn(onComplete: Function1[Any,Unit]): Boolean = {
			val f = fn()
			val done = f.isCompleted
			if (!done) f.onComplete(onComplete)
			done
		}
	}

	case class BackpressureEnqueueOnly[A](fn: Function0[A])
		extends AsyncTask with HasEnqueuePromise[Unit] with IsEnqueueOnly {

		override def spawn(onComplete: Function1[Any,Unit]): Boolean = {
			val f = fn()
			true
		}
	}
}
