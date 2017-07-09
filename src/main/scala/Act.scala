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
		new SequentialState[T](Lwt(), v)

	def apply[T](v: T, lwt: Lwt)(implicit ec: ExecutionContext) =
		new SequentialState[T](lwt, v)
}

class SequentialState[T](thread: Lwt, init: T) {
	private val state = new Ref(init)
	// private val thread = new Lwt(bufLen)(ec)

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

//// Supervisor strategy: I guess just have an uncaughtHandler per LWT?
//// You should be using Try instead of exceptions anyway...

case class UnitOfWork[A](fn: Function0[A]) {
	val promise = Promise[A]()
	def future = promise.future
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


	def popTask(state: ThreadState):(Option[UnitOfWork[_]],ThreadState) = {
		if (state.tasks.isEmpty) {
			(None, state.park())
		} else {
			val (head, tail) = state.tasks.dequeue
			(Some(head), new ThreadState(tail, state.waiters, state.running))
		}
	}

	def popWaiter(bufLen: Int)(state: ThreadState):(Option[Waiter[_]],ThreadState) = {
		if (state.waiters.isEmpty || state.tasks.size >= bufLen) {
			(None, state)
		} else {
			val (head, tail) = state.waiters.dequeue
			(Some(head), new ThreadState(state.tasks.enqueue(head.task), tail, state.running))
		}
	}

	def empty = new ThreadState(Queue.empty[UnitOfWork[_]], Queue.empty[Waiter[_]], false)

	def start(state: ThreadState): (Boolean, ThreadState) = {
		(!state.running, new ThreadState(state.tasks, state.waiters, true))
	}
}

class ThreadState(val tasks: Queue[UnitOfWork[_]], val waiters: Queue[Waiter[_]], val running: Boolean) {
	import ThreadState._

	def enqueueTask(task: UnitOfWork[_]): ThreadState = {
		new ThreadState(tasks.enqueue(task), waiters, true)
	}

	def enqueueWaiter(waiter: Waiter[_]): ThreadState = {
		assert(running)
		new ThreadState(tasks, waiters.enqueue(waiter), running)
	}

	def length = tasks.length

	def park() = {
		assert(running)
		new ThreadState(tasks, waiters, false)
	}
}

class Waiter[A](
	val task: UnitOfWork[A],
	promise: Promise[Future[A]]) {
	def enqueued() {
		promise.success(task.future)
	}
}

object Lwt {
	val defaultBufferSize = 20
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new Lwt(bufLen)
}

class Lwt(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = Atomic(ThreadState.empty)

	val workLoop:Runnable = new Runnable() {
		// println("running")
		private val popWaiter: Function[ThreadState,(Option[Waiter[_]],ThreadState)] = ThreadState.popWaiter(bufLen)
		def run() {
			@tailrec def loop(maxIterations: Int): Unit = {
				val task = state.transformAndExtract(ThreadState.popTask)
				task match {
					case None => ()
					case Some(task) => {
						// evaluate this function on this fiber
						task.run()

						// and promote a waiting task, if any
						state.transformAndExtract(popWaiter).foreach { waiter =>
							// println("popped a waiter!")
							waiter.enqueued()
						}

						if (maxIterations == 0) {
							// re-enqueue the runnable instead of looping to prevent starvation
							ec.execute(workLoop)
						} else {
							loop(maxIterations - 1)
						}
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

	private def autoSchedule(enqueueResult: EnqueueResult): Boolean = {
		// println(s"autoSchedule: $enqueueResult")
		if(enqueueResult.shouldSchedule) {
			// println("scheduling parked thread")
			ec.execute(workLoop)
		}
		enqueueResult.success
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		// println("enqueueAsync() called")
		val task = UnitOfWork(fun)

		// try an immediate enqueue:
		val done = autoSchedule(state.transformAndExtract { state =>
			if (state.tasks.length < bufLen) {
				(EnqueueResult.successful(state.running), state.enqueueTask(task))
			} else {
				assert(state.running, "thread is full but not running!")
				(EnqueueResult.rejected, state)
			}
		})

		if (done) {
			//easy mode:
			// println("enqueueAsync completed immediately")
			Future.successful(task.future)
		} else {
			// slow mode: make a promise and hop in the waiting queue if
			// tasks is (still) full
			val enqueued = Promise[Future[A]]()
			val waiter = new Waiter[A](task, enqueued)
			val done = autoSchedule(state.transformAndExtract { state =>
				if (state.length < bufLen) {
					(EnqueueResult.successful(state.running), state.enqueueTask(task))
				} else {
					assert(state.running, "state should be running!")
					(EnqueueResult.rejected, state.enqueueWaiter(waiter))
				}
			})
			if (done) {
				waiter.enqueued()
			}
			enqueued.future
		}
	}
}

