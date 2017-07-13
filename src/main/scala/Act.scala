import java.nio.charset.Charset

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
	var enqueuedPromise:Promise[Future[A]] = null
	def future = promise.future

	def makeEnqueueable(): Unit = {
		enqueuedPromise = Promise()
	}

//	def tryEnqueue(state:ThreadState) = {
//		if (state.hasSpace(bufLen)) {
//			state.enqueueTask(this)
//		} else {
//			// assert(state.running, "thread is full but not running!")
//			state
//		}
//	}

	def enqueue(state:ThreadState) = {
		if (state.hasSpace(bufLen)) {
      state.enqueueTask(this)
		} else {
			if (enqueuedPromise == null) {
				// mutation is safe since during enqueue only the enqueueing thread
				// has a reference to this object
				enqueuedPromise = Promise()
			}
      state.enqueueWaiter(this)
		}
	}

	def enqueued() {
		enqueuedPromise.success(future)
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
	def popTask(state: ThreadState):ThreadState = {
		if (state.tasks.isEmpty) {
			state.park()
		} else {
			var numTasks = state.numTasks
			if (!state.hasWaiters) {
				numTasks -= 1
			}
      new ThreadState(state.tasks.tail, state.running, numTasks)
		}
	}

	def empty(capacity: Int) = new ThreadState(Vector.empty[UnitOfWork[_]], false, 0)

	def start(state: ThreadState): (Boolean, ThreadState) = {
		(!state.running, new ThreadState(state.tasks, true, state.numTasks))
	}
}

class ThreadState(val tasks: Vector[UnitOfWork[_]], val running: Boolean, val numTasks: Int) {

	def hasWaiters:Boolean = tasks.length > numTasks

	def markWaiterEnqueued() {
		if (hasWaiters) {
      tasks(numTasks).enqueued()
		}
	}

	def hasSpace(capacity: Int) = numTasks < capacity

	def enqueueTask(task: UnitOfWork[_]): ThreadState = {
//		assert(numWaiters == 0)
		new ThreadState(tasks.:+(task), true, numTasks + 1)
	}

	def enqueueWaiter(task: UnitOfWork[_]): ThreadState = {
//		assert(running)
		new ThreadState(tasks.:+(task), running, numTasks)
	}

	def park() = {
		// assert(running)
		new ThreadState(tasks, false, numTasks)
	}
}

object SequentialExecutor {
	val defaultBufferSize = 5
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = Atomic(ThreadState.empty(bufLen))

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
					oldState.markWaiterEnqueued()
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
		val task = UnitOfWork(fun, bufLen)
		val prevState = state.getAndTransform(task.enqueue)
		if (prevState.hasSpace(bufLen)) {
			// enqueued a task
			if(!prevState.running) {
				ec.execute(workLoop)
			}
			// println("enqueueAsync completed immediately")
			Future.successful(task.future)
		} else {
			// enqueued a waiter
      task.enqueuedPromise.future
		}
	}
}

