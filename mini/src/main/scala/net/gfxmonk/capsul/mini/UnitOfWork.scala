package net.gfxmonk.capsul.mini

import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

trait Work {
	def run(): Unit
}

trait AsyncWork {
	def runAsync(onComplete: Function0[Unit], schedulr: Scheduler): Unit
}

private class AsyncWorkIgnore[T](val task: Task[T]) extends AsyncWork {
	val enqueuedPromise = Promise[Unit]()
	override def runAsync(onComplete: Function0[Unit], scheduler: Scheduler): Unit = {
		enqueuedPromise.success(())
		AsyncWorkIgnore.runAsync(task, onComplete, scheduler)
	}
}

object AsyncWorkIgnore {
	def runAsync[T](task: Task[T], onComplete: Function0[Unit], scheduler: Scheduler): Unit = {
		task.runOnComplete { result =>
			onComplete()
		}(scheduler)
	}
}

private [capsul] class AsyncWorkReturn[T](val task: Task[T], promise: Promise[T]) extends AsyncWork {
	val enqueuedPromise = Promise[Future[T]]()

	override def runAsync(onComplete: Function0[Unit], scheduler: Scheduler): Unit = {
		AsyncWorkReturn.runAsync(task, promise, onComplete, scheduler)
		enqueuedPromise.success(promise.future)
	}
}

private [capsul] object AsyncWorkReturn {
	def runAsync[T](task: Task[T], promise: Promise[T], onComplete: Function0[Unit], scheduler: Scheduler): Unit = {
		task.runOnComplete { result =>
			onComplete()
			promise.complete(result)
		}(scheduler)
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
