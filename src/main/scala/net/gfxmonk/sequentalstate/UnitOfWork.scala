import java.util.concurrent.locks.LockSupport
import monix.execution.misc.NonFatal
import scala.concurrent.{Future, Promise}

case class UnitOfWork[A](fn: Function0[A], bufLen: Int) {
	val resultPromise = Promise[A]()
	var enqueuedPromise: Promise[Future[A]] = null

	def enqueuedAsync() {
		enqueuedPromise.success(resultPromise.future)
	}

	def tryEnqueue(state:ExecutorState) = {
		if (state.hasSpace(bufLen)) {
			state.enqueueTask(this)
		} else {
			// we actually want to lose this commit race if we're fighting with the run thread
			LockSupport.parkNanos(1000)
			if (enqueuedPromise == null) {
				enqueuedPromise = Promise()
			}
			state.enqueueWaiter()
		}
	}

	def run() = {
		try {
			resultPromise.success(fn())
		} catch {
			case e:Throwable => {
				if (NonFatal(e)) {
					resultPromise.failure(e)
				} else {
					throw e
				}
			}
		}
	}
}
