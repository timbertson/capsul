import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

class Ref[T](init:T) {
	@volatile private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}

object SequentialState {
	def apply[T](v: T)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, SequentialExecutor())

	def apply[T](v: T, thread: SequentialExecutor)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, thread)
}

class SequentialState[T](init: T, thread: SequentialExecutor) {
	private val state = new Ref(init)
	// Naming:
	//
	//  - send*:    Enqueue an action, returning Future[Unit]. Once the future is resolved, the
	//              action has been accepted by the worker, but not yet performed.
	//
	//  - *:        Perform an action and return its result. The future resolves once
	//              the action is complete.
	//
	//  - raw*:     Perform an action, returning a Future[Future[T]]. The outer future
	//              resolves once the task is enqueued (at which point futher tasks may
	//              be enqueued). The inner future resolves once the task is completed.
	//
	// Action types:
	//
	// mutate(fn: Function[State[T],R]
	// transform(fn: Function[T,T])
	// set(value: T)
	// access: Function[T,R] -> accepts a mutable Ref for both reading and setting.

	// sendMutate: omitted; sendMap is just as functional
	def sendTransform(fn: Function[T,T]):       Future[Unit]         = thread.enqueueOnly(() => state.set(fn(state.current)))
	def sendSet(updated: T):                    Future[Unit]         = thread.enqueueOnly(() => state.set(updated))
	def sendAccess(fn: Function[T,_]):          Future[Unit]         = thread.enqueueOnly(() => fn(state.current))

	def awaitMutate[R](fn: Function[Ref[T],R]): Future[R]            = thread.enqueueReturn(() => fn(state))
	def awaitTransform[R](fn: Function[T,T]):   Future[Unit]         = thread.enqueueReturn(() => state.set(fn(state.current)))
	def awaitSet[R](updated: T):                Future[Unit]         = thread.enqueueReturn(() => state.set(updated))
	def awaitAccess[R](fn: Function[T,R]):      Future[R]            = thread.enqueueReturn(() => fn(state.current))
	def current:                                Future[T]            = thread.enqueueReturn(() => state.current)

	def rawMutate[R](fn: Function[Ref[T],R]):   Future[Future[R]]    = thread.enqueueAsync(() => fn(state))
	def rawAccess[R](fn: Function[T,R]):        Future[Future[R]]    = thread.enqueueAsync(() => fn(state.current))
}

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

object ThreadState {
	def popTask(state: ThreadState):ThreadState = {
		if (!state.hasTasks) {
			state.park()
		} else {
			val numTasks = state.numTasks - 1
			val numWaiters = Math.max(state.numWaiters - 1, 0)
      new ThreadState(state.tasks.tail, state.running, numTasks, numWaiters)
		}
	}

	def empty = new ThreadState(Queue.empty[UnitOfWork[_]], false, 0, 0)
}

class ThreadState(val tasks: Queue[UnitOfWork[_]], val running: Boolean, val numTasks: Int, val numWaiters: Int) {
	def hasTasks = numTasks != 0
	def hasWaiters = numWaiters != 0
	def hasSpace(capacity: Int) = numTasks < capacity

	def nextWaiter = {
    val taskIdx = numWaiters % numTasks
    tasks(taskIdx).resultPromise.future
	}

	def enqueueTask(task: UnitOfWork[_]): ThreadState = {
//		assert(numWaiters == 0)
		new ThreadState(tasks.enqueue(task), true, numTasks + 1, numWaiters)
	}

	def enqueueWaiter(): ThreadState = {
//		assert(running)
		new ThreadState(tasks, running, numTasks, numWaiters + 1)
	}

	def park() = {
		// assert(running)
		new ThreadState(tasks, false, numTasks, numWaiters)
	}
}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = Atomic(ThreadState.empty)

	val workLoop:Runnable = new Runnable() {
		def run() {
			@tailrec def loop(maxIterations: Int): Unit = {
				val oldState:ThreadState = state.getAndTransform(ThreadState.popTask)

				if (oldState.tasks.nonEmpty) {
					// we popped a task!
					oldState.tasks.head.run()
          if (maxIterations == 0) {
            // re-enqueue the runnable instead of looping to prevent starvation
            ec.execute(workLoop)
          } else {
            loop(maxIterations - 1)
          }
				}
			}
			loop(200)
		}
	}

	def enqueueOnly[R](fun: Function0[R]): Future[Unit] = {
		enqueueAsync(fun).map((_:Future[R]) => ())
	}

	def enqueueReturn[R](fun: Function0[R]): Future[R] = {
		enqueueAsync(fun).flatMap(identity)
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork(fun, bufLen, this)
		if (enqueue(task)) {
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}

	// TODO: private
	def enqueue[A](task: UnitOfWork[A]): Boolean = {
		val prevState = state.getAndTransform(task.tryEnqueue)
		if (prevState.hasSpace(bufLen)) {
			if(!prevState.running) {
				ec.execute(workLoop)
			}
			true
		} else {
			task.delayEnqueue(prevState)
			false
		}
	}
}

