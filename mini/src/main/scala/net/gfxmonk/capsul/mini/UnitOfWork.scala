package net.gfxmonk.capsul.mini

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

trait Work {
	def run(): Unit
}

trait AsyncWork {
	def runAsync(onComplete: Function0[Unit], ec: ExecutionContext): Unit
}

private class AsyncWorkIgnore[T](val task: Function0[Future[T]]) extends AsyncWork {
	val enqueuedPromise = Promise[Unit]()
	override def runAsync(onComplete: Function0[Unit], ec: ExecutionContext): Unit = {
		enqueuedPromise.success(())
		AsyncWorkIgnore.runAsync(task, onComplete, ec)
	}
}

object AsyncWorkIgnore {
	def runAsync[T](task: Function0[Future[T]], onComplete: Function0[Unit], ec: ExecutionContext): Unit = {
		task().onComplete { result =>
			onComplete()
		}(ec)
	}
}

private [capsul] class AsyncWorkReturn[T](val task: Function0[Future[T]], promise: Promise[T]) extends AsyncWork {
	val enqueuedPromise = Promise[Future[T]]()

	override def runAsync(onComplete: Function0[Unit], ec: ExecutionContext): Unit = {
		AsyncWorkReturn.runAsync(task, promise, onComplete, ec)
		enqueuedPromise.success(promise.future)
	}
}

private [capsul] object AsyncWorkReturn {
	def runAsync[T](task: Function0[Future[T]], promise: Promise[T], onComplete: Function0[Unit], ec: ExecutionContext): Unit = {
		task().onComplete { result =>
			onComplete()
			promise.complete(result)
		}(ec)
	}
}

private [capsul] class WorkIgnore[A](fn: Function0[A]) extends Work {
	final override def run(): Unit = {
		fn()
	}
}

private [capsul] class WorkReturn[A](fn: Function0[A]) extends Work {
	val resultPromise: Promise[A] = Promise[A]()
	final override def run(): Unit = {
		try {
			resultPromise.success(fn())
		} catch {
			case NonFatal(error) => {
				resultPromise.failure(error)
			}
		}
	}
}
