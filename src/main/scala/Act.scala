import java.nio.charset.Charset
import java.util.concurrent.locks.LockSupport

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

trait UnitOfWork[A] {
	val fn: Function0[A]
	val bufLen: Int

	def onEnqueue(): Unit
	def enqueued(): Unit
	def reportSuccess(result: A): Unit
	def reportFailure(error: Throwable): Unit

	def enqueue(state:ThreadState) = {
		if (state.hasSpace(bufLen)) {
			state.enqueueTask(this)
		} else {
			// we actually want to lose this commit race if we're fighting with the run thread
			LockSupport.parkNanos(1000)
			this.onEnqueue()
			state.enqueueWaiter(this)
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

object UnitOfWork {
  case class Full[A](fn: Function0[A], bufLen: Int) extends UnitOfWork[A] {
		val resultPromise = Promise[A]()

		def reportSuccess(result: A) = {
			resultPromise.success(result)
		}

		def reportFailure(error: Throwable): Unit = {
			resultPromise.failure(error)
		}

		var enqueuedPromise: Promise[Future[A]] = null
		def onEnqueue(): Unit = {
			if (enqueuedPromise == null) {
				enqueuedPromise = Promise()
			}
		}

		def enqueued() {
			enqueuedPromise.success(resultPromise.future)
		}
	}

  case class EnqueueOnly[A](fn: Function0[A], bufLen: Int) extends UnitOfWork[A] {
		override def reportSuccess(result: A): Unit = ()
		override def reportFailure(error: Throwable): Unit = ()

		var enqueuedPromise: Promise[Unit] = null

		def onEnqueue(): Unit = {
			if (enqueuedPromise == null) {
				enqueuedPromise = Promise()
			}
		}

		def enqueued() {
			enqueuedPromise.success(())
		}
	}

	case class ReturnOnly[A](fn: Function0[A], bufLen: Int) extends UnitOfWork[A] {
		override def onEnqueue(): Unit = ()
		override def enqueued(): Unit = ()

		val resultPromise = Promise[A]()

		def reportSuccess(result: A) = {
			resultPromise.success(result)
		}

		def reportFailure(error: Throwable): Unit = {
			resultPromise.failure(error)
		}
	}
}

object ThreadState {
	def popTask(state: ThreadState):ThreadState = {
		if (state.tasks.isEmpty) {
			state.park()
		} else {
			val numTasks = if (state.hasWaiters) state.numTasks else state.numTasks - 1
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

	val workLoop:Runnable = new Runnable() {
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

//		val task = UnitOfWork.EnqueueOnly(fun, bufLen)
//		if (enqueue(task)) {
//			Future.successful(())
//		} else {
//			task.enqueuedPromise.future
//		}
	}

	def enqueueReturn[R](fun: Function0[R]): Future[R] = {
//		enqueueAsync(fun).flatMap(identity)

		val task = UnitOfWork.ReturnOnly(fun, bufLen)
		enqueue(task)
		task.resultPromise.future
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork.Full(fun, bufLen)

		if (enqueue(task)) {
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}

	private def enqueue[A](task: UnitOfWork[A]): Boolean = {
		val prevState = state.getAndTransform(task.enqueue)
		if (prevState.hasSpace(bufLen)) {
			// enqueued a task
//			println("executed immeditaly")
			if(!prevState.running) {
				ec.execute(workLoop)
			}
			true
		} else {
//			println("waiting...")
			false
		}
	}
}

