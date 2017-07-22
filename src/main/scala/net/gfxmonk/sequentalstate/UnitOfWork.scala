import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

case class UnitOfWork[A](fn: Function0[A], bufLen: Int, dest: SequentialExecutor) {
	val resultPromise = Promise[A]()

	def reportSuccess(result: A) = {
		resultPromise.success(result)
	}

	def reportFailure(error: Throwable): Unit = {
		resultPromise.failure(error)
	}

	var enqueuedPromise: Promise[Future[A]] = null

	def enqueued() {
		enqueuedPromise.success(resultPromise.future)
	}

	def delayEnqueue(state: ThreadState)(implicit ec: ExecutionContext) = {
		if (enqueuedPromise == null) {
			enqueuedPromise = Promise()
		}
		state.nextWaiter.onComplete { _ =>
			if (dest.enqueue(this)) {
				this.enqueued()
			}
		}
	}

	def tryEnqueue(state:ThreadState) = {
		if (state.hasSpace(bufLen)) {
			state.enqueueTask(this)
		} else {
			// we actually want to lose this commit race if we're fighting with the run thread
			LockSupport.parkNanos(1000)
			state.enqueueWaiter()
		}
	}

	def run() = {
		try {
			reportSuccess(fn())
//			println("success: " + this)
		} catch {
			case e:Throwable => {
				if (NonFatal(e)) {
					reportFailure(e)
//					println("failure: " + this)
				} else {
					throw e
				}
			}
		}
	}
}
