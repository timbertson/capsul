import java.nio.charset.Charset

import ThreadState.EnqueueResult
import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Success, Try}

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
	// map(fn: Function[T,T])
	// set(value: T)
	// access: Function[T,R] -> accepts a mutable Ref for both reading and setting.

	// sendMutate: omitted; sendMap is just as functional
	def sendMap(fn: Function[T,T]):             Future[Unit]         = thread.enqueueOnly(() => state.set(fn(state.current)))
	def sendSet(updated: T):                    Future[Unit]         = thread.enqueueOnly(() => state.set(updated))
	def sendAccess(fn: Function[T,_]):          Future[Unit]         = thread.enqueueOnly(() => fn(state.current))

	def awaitMutate[R](fn: Function[Ref[T],R]): Future[R]            = thread.enqueueReturn(() => fn(state))
	def awaitMap[R](fn: Function[T,T]):         Future[Unit]         = thread.enqueueReturn(() => state.set(fn(state.current)))
	def awaitSet[R](updated: T):                Future[Unit]         = thread.enqueueReturn(() => state.set(updated))
	def awaitAccess[R](fn: Function[T,R]):      Future[R]            = thread.enqueueReturn(() => fn(state.current))
	def current:                                Future[T]            = thread.enqueueReturn(() => state.current)

	def rawMutate[R](fn: Function[Ref[T],R]):   Future[Future[R]]    = thread.enqueueAsync(() => fn(state))
	def rawAccess[R](fn: Function[T,R]):        Future[Future[R]]    = thread.enqueueAsync(() => fn(state.current))
}

//// Supervisor strategy: I guess just have an uncaughtHandler per thread?
//// You should be using Try instead of exceptions anyway...

case class UnitOfWork[A](fn: Function0[A], bufLen: Int) {
	val promise = Promise[A]()
	def future = promise.future

	def tryEnqueue(state:ThreadState) = {
		if (state.hasSpace(bufLen)) {
			(EnqueueResult.successful(state.running), state.enqueueTask(this))
		} else {
			// assert(state.running, "thread is full but not running!")
			(EnqueueResult.rejected, state)
		}
	}

	def run() = {
		try {
			promise.success(fn())
		} catch {
			case e:Throwable => {
				if (NonFatal(e)) {
					promise.failure(e)
				} else {
					throw e
				}
			}
		}
	}
}


object ThreadState {
	class EnqueueResult(val success: Boolean, val wasRunning: Boolean) {
		def shouldSchedule = success && !wasRunning
		override def toString() = {
			s"EnqueueResult(success=$success, wasRunning=$wasRunning)"
		}
	}

	// pre-bind the combinations we need
	val ENQUEUED_ALREADY_RUNNING = new EnqueueResult(success = true, wasRunning = true)
	val ENQUEUED_NOT_RUNNING = new EnqueueResult(success = true, wasRunning = false)
	val ENQUEUE_REJECTED = new EnqueueResult(success = false, wasRunning = true)

	object EnqueueResult {
		def successful(running: Boolean) = {
			if (running) ENQUEUED_ALREADY_RUNNING else ENQUEUED_NOT_RUNNING
		}

		def rejected = ENQUEUE_REJECTED
	}


	def popTask(state: ThreadState):ThreadState = {
		if (state.tasks.isEmpty) {
			state.park()
		} else {
			var (task, tasks) = state.tasks.dequeue
			var waitIdx = state.waitIdx
			if (state.hasWaiters) {
				waitIdx = state.waitIdx + 1
			}
      new ThreadState(tasks, state.waiters, waitIdx, state.running)
		}
	}

//	def popTasks(state: ThreadState):(Option[ (Queue[UnitOfWork[_]], Queue[Waiter[_]]) ],ThreadState) = {
//		if (state.tasks.isEmpty) {
//			(None, state.park())
//		} else {
//			val (poppedWaiters, remainingWaiters) = state.waiters.splitAt(state.tasks.length)
//			(Some((state.tasks, poppedWaiters)), new ThreadState(Queue.empty, remainingWaiters, state.running))
//		}
//	}

//	def popWaiter(bufLen: Int)(state: ThreadState):(Option[Waiter[_]],ThreadState) = {
//		if (state.waiters.isEmpty || state.tasks.size >= bufLen) {
//			(None, state)
//		} else {
//			val (head, tail) = state.waiters.dequeue
//			(Some(head), new ThreadState(state.tasks.enqueue(head.task), tail, state.running))
//		}
//	}

		def popWaiter(state: ThreadState):(Option[Waiter[_]],ThreadState) = {
			if (state.waitIdx > 0) {
				val (head, tail) = state.waiters.dequeue
				(Some(head), new ThreadState(state.tasks.enqueue(head.task), tail, state.waitIdx - 1, state.running))
			} else {
				(None, state)
			}
		}

	val empty = new ThreadState(Queue.empty[UnitOfWork[_]], Queue.empty[Waiter[_]], 0, false)

	def start(state: ThreadState): (Boolean, ThreadState) = {
		(!state.running, new ThreadState(state.tasks, state.waiters, state.waitIdx, true))
	}
}

class ThreadState(
	val tasks: Queue[UnitOfWork[_]],
	val waiters: Queue[Waiter[_]],
	val waitIdx: Int,
	val running: Boolean
) {
	def hasWaiters = if (waitIdx == 0) waiters.nonEmpty else waiters.length > waitIdx
	def hasSpace(capacity: Int) = tasks.length + waitIdx < capacity
	def firstWaiter = waiters(waitIdx)

	def enqueueTask(task: UnitOfWork[_]): ThreadState = {
		// enqueues a task, also drops waiters once they reach a limit
		var newWaiters = waiters
		var newWaitIdx = waitIdx
//		if (waitIdx > 10) {
//      newWaiters = waiters.drop(waitIdx)
//      newWaitIdx = 0
//		}
		new ThreadState(tasks.enqueue(task), newWaiters, newWaitIdx, true)
	}

	def enqueueWaiter(waiter: Waiter[_]): ThreadState = {
		// assert(running)
		new ThreadState(tasks, waiters.enqueue(waiter), waitIdx, running)
	}

	def park() = {
		// assert(running)
		new ThreadState(tasks, waiters, waitIdx, false)
	}
}

class Waiter[A](
	val task: UnitOfWork[A],
	promise: Promise[Future[A]]) {
	def enqueued() {
		promise.success(task.future)
	}
	def isEnqueued = promise.isCompleted

	def enqueue(state:ThreadState) = {
		if (state.hasSpace(task.bufLen)) {
			(EnqueueResult.successful(state.running), state.enqueueTask(task))
		} else {
			// assert(state.running, "state should be running!")
			(EnqueueResult.rejected, state.enqueueWaiter(this))
		}
	}

}

object SequentialExecutor {
	val defaultBufferSize = 5
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = Atomic(ThreadState.empty)

	def logCallCount[A,B](fn:Function[A,B]):Function[A,B] = {
		var count = 0
		(a) => {
			count += 1
			if (count>10) println("."+count)
			fn(a)
		}
	}


	val workLoop:Runnable = new Runnable() {
		// println("running")
//		private val popWaiter: Function[ThreadState,(Option[Waiter[_]],ThreadState)] = ThreadState.popWaiter(bufLen)
		def run() {
			@tailrec def loop(maxIterations: Int): Unit = {
				val oldState:ThreadState = state.getAndTransform(ThreadState.popTask)

				if (oldState.tasks.nonEmpty) {
					// we popped a task!
					// evaluate the task on this fiber
					oldState.tasks.head.run()

					val waiter = state.transformAndExtract(ThreadState.popWaiter)
					waiter.foreach(_.enqueued())

//					// we _may_ have popped a waiter too:
//					if (oldState.hasWaiters) {
//						oldState.firstWaiter.enqueued()
//					}

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

	private def autoSchedule(enqueueResult: EnqueueResult): Boolean = {
		// println(s"autoSchedule: $enqueueResult")
		if(enqueueResult.shouldSchedule) {
			// println("scheduling parked thread")
			ec.execute(workLoop)
		}
		enqueueResult.success
	}

	def enqueueOnly[R](fun: Function0[R]): Future[Unit] = {
		enqueueAsync(fun).map((_:Future[R]) => ())
	}

	def enqueueReturn[R](fun: Function0[R]): Future[R] = {
		enqueueAsync(fun).flatMap(identity)
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		import ThreadState.EnqueueResult
		// println("enqueueAsync() called")
		val task = UnitOfWork(fun, bufLen)

		// try an immediate enqueue:
		val done = autoSchedule(state.transformAndExtract(task.tryEnqueue))

		if (done) {
			//easy mode:
			// println("enqueueAsync completed immediately")
			Future.successful(task.future)
		} else {
			// slow mode: make a promise and hop in the waiting queue if
			// tasks is (still) full
			val enqueued = Promise[Future[A]]()
			val waiter = new Waiter[A](task, enqueued)

			val done = autoSchedule(state.transformAndExtract(waiter.enqueue))
			if (done) {
				waiter.enqueued()
			}
			enqueued.future
		}
	}
}

