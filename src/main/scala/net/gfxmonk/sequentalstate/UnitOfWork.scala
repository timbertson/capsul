package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import monix.execution.misc.NonFatal
import scala.concurrent.{Future, Promise}

trait UnitOfWork[A] {
	protected val fn: Function0[A]
	protected val bufLen: Int

	def enqueuedAsync(): Unit
	// protected def prepareForAsyncEnqueue(): Unit
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
	trait HasEnqueuePromise[A] {
		var _enqueuedPromise: Promise[A] = null
		def enqueuedPromise: Promise[A] = {
			if (_enqueuedPromise == null) {
				_enqueuedPromise = Promise()
			}
			_enqueuedPromise
		}
	}

	trait HasResultPromise[A] {
		val resultPromise: Promise[A] = Promise[A]()

		final def reportSuccess(result: A): Unit = {
			resultPromise.success(result)
		}

		final def reportFailure(error: Throwable): Unit = {
			resultPromise.failure(error)
		}
	}

	case class Full[A](fn: Function0[A], bufLen: Int)
		extends UnitOfWork[A]
		with HasEnqueuePromise[Future[A]]
		with HasResultPromise[A]
	{
		final def enqueuedAsync() {
			enqueuedPromise.success(resultPromise.future)
		}
	}

	case class EnqueueOnly[A](fn: Function0[A], bufLen: Int)
		extends UnitOfWork[A] with HasEnqueuePromise[Unit]
	{
		final def reportSuccess(result: A): Unit = ()
		final def reportFailure(error: Throwable): Unit = ()

		final def enqueuedAsync() {
			enqueuedPromise.success(())
		}
	}

	case class ReturnOnly[A](fn: Function0[A], bufLen: Int)
		extends UnitOfWork[A] with HasResultPromise[A]
	{
		// final def prepareForAsyncEnqueue(): Unit = ()
		final def enqueuedAsync(): Unit = ()
	}
}
