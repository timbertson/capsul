package net.gfxmonk.capsul.cml

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import net.gfxmonk.capsul.internal.Log
import net.gfxmonk.capsul.StagedFuture
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import scala.concurrent.{ExecutionContext, Future}

class Capsul[T](initial:T, val limit: Int)(implicit ec: ExecutionContext) {
	import Capsul._

	private val state = new Ref[T](initial)
	private val queue = new ConcurrentLinkedQueue[EnqueueableOp]()

	// runState is:
	// xxxx|xxxx
	// ^ outstanding futures
	//       ^ queued tasks
	//
	// When we change from 0 to 1 queued task, we run. When shutting down, we always have at
	// least one slot to free, so the CAS ensures we are not confused about when we have
	// transitioned to the stopped state.
	//
	// When we get to `limit` outstanding futures, we also suspend. In that case each future's
	// onComplete will reschedule if (a) there is work queued, and (b) there were `limit` outstanding futures

	private val runState = new AtomicInteger(0)

	/* == API == */

	import UnitOfWork._
	def enqueueOnly(task: EnqueueableOp with HasEnqueuePromise[Unit]) = {
		// TODO: optimize
		Op(submit(task))
		task.enqueuedPromise.future
	}

	def enqueue[T](task: EnqueueableOp
			with UnitOfWork.HasEnqueuePromise[Future[T]]
			with UnitOfWork.HasResultPromise[T]
	): StagedFuture[T] = {
		// TODO: optimize
		Op(submit(task))
		StagedFuture(task.enqueuedPromise.future)
		// if (doEnqueue(task)) {
		// 	StagedFuture.accepted(task.resultPromise.future)
		// } else {
		// 	StagedFuture(task.enqueuedPromise.future)
		// }
	}

	/** Send a pure transformation */
	def sendTransform(fn: T => T): Future[Unit] =
		enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(fn(state.current))))

	/** Send a set operation */
	def sendSet(updated: T): Future[Unit] =
		enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(updated)))

	/** Send an access operation */
	def sendAccess(fn: T => _): Future[Unit] =
		enqueueOnly(UnitOfWork.EnqueueOnly(() => fn(state.current)))

	/** Send an access operation which returns a [[StagedFuture]][R] */
	def sendAccessStaged[A](fn: T => StagedFuture[A])(implicit ec: ExecutionContext): Future[Unit] =
		enqueueOnly(UnitOfWork.EnqueueOnlyStaged(() => fn(state.current)))

	/** Send an access operation which returns a [[Future]][R] */
	def sendAccessAsync[A](fn: T => Future[A]): Future[Unit] =
		enqueueOnly(UnitOfWork.EnqueueOnlyAsync(() => fn(state.current)))

	/** Return the current state value */
	def current: Future[T] =
		enqueue(UnitOfWork.Full(() => state.current))

	/** Perform a full mutation */
	def mutate[R](fn: Ref[T] => R): StagedFuture[R] =
		enqueue(UnitOfWork.Full(() => fn(state)))

	/** Perform a pure transformation */
	def transform(fn: T => T): StagedFuture[T] =
		enqueue(UnitOfWork.Full { () =>
			val updated = fn(state.current)
			state.set(updated)
			updated
		})

	/** Perform a function with the current state */
	def access[R](fn: T => R): StagedFuture[R] =
		enqueue(UnitOfWork.Full(() => fn(state.current)))

	/** Perform a mutation which returns a [[StagedFuture]][R] */
	def mutateStaged[R](fn: Ref[T] => StagedFuture[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		enqueue(new UnitOfWork.FullStaged[R](() => fn(state)))

	/** Perform a mutation which returns a [[Future]][R] */
	def mutateAsync[R](fn: Ref[T] => Future[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		enqueue(new UnitOfWork.FullAsync[R](() => fn(state)))

	/** Perform an access which returns a [[StagedFuture]][R] */
	def accessStaged[R](fn: T => StagedFuture[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		enqueue(new UnitOfWork.FullStaged[R](() => fn(state.current)))

	/** Perform an access which returns a [[Future]][R] */
	def accessAsync[R](fn: T => Future[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		enqueue(new UnitOfWork.FullAsync[R](() => fn(state.current)))


	private def submit[R](task: EnqueueableOp): Op[Unit] = {
		new Submit(this, task, ec)
	}


	// Note: doesn't need to be volatile, since we
	// always write to it before reading in the same thread
	private var buffer = new Array[EnqueueableOp](limit)

	@tailrec private def acknowledgeAndBuffer(alreadyCompleted: Int): Int = {
		val state = runState.get()

		// we can acknowledge up to limit tasks, minus outstanding futures
		val availableSyncTasks = Math.min(limit, numQueued(state) - alreadyCompleted) - numFutures(state)
		if (availableSyncTasks == 0) {
			if (alreadyCompleted == 0) {
				// we're done, shut down
				0
			} else {
				// nothing to do, but we've still got queued items
				if (runState.compareAndSet(state, state - alreadyCompleted)) {
					// no conflict, shut down
					0
				} else {
					// conflict, retry
					acknowledgeAndBuffer(alreadyCompleted)
				}
			}
		} else {
			var acknowledgedSyncTasks = 0

			// acknowledge new tasks _before_ decrementing state, to ensure enqueuer won't acknowledge tasks later in the queue
			while (acknowledgedSyncTasks == 0) {
				// loop until we've acknowledged at least one task
				// val it = queue.iterator()
				// TODO we could go up to `limit - numFutures`, rather than cap at availableSyncTasks
				var item = queue.poll()
				while(acknowledgedSyncTasks < availableSyncTasks) {
					while(item == null) {
						// there must be an item eventually, we were promised!
						item = queue.poll()
					}
					// TODO: can we only enqueuedAsync for those items where it hasn't been called yet?
					item.enqueuedAsync()
					buffer.update(acknowledgedSyncTasks, item)
					acknowledgedSyncTasks += 1
				}
			}
			if (alreadyCompleted != 0) {
				runState.addAndGet(-alreadyCompleted)
			}
			acknowledgedSyncTasks
		}
	}

	@tailrec private def runLoop(acknowledgedSyncTasks: Int) {
		import Log._
		val logId = Log.scope(self, "WorkLoop.runLoop")
		// perform all acknowledged tasks
		var taskIdx = 0
		while (taskIdx < acknowledgedSyncTasks) {
			// TODO: try to decrement state once fullyCompletedItems exceeds limit/2
			// (also acknowledge more tasks as we go)
			
			var work = buffer(taskIdx)
			taskIdx += 1

			// TODO: handle CAS for work with a non-null opState
			work.run().filter(!_.isCompleted) match {
				case None => {
					log("ran sync node")
				}
				case Some(f) => {
					log("ran async node")
					self.runState.getAndAdd(SINGLE_FUTURE)
					f.onComplete { _ =>
						val prevState = self.runState.addAndGet(-SINGLE_FUTURE)
						assert(numFutures(prevState) <= limit)
						if (numFutures(prevState) == limit && numQueued(prevState) > 0) {
							// we just freed up a slot to deque a pending task into. The loop
							// was stopped (waiting for futures), so we should run it again
							ec.execute(workLoop)
						}
					}
				}
			}
			buffer.update(taskIdx, null) // enable GC
		}

		// loop again, if there are more tasks
		// We've done acknowledgedSyncTasks tasks, but haven't updated state yet.
		// First, see if there's more to do
		val newlyAcknowledgedTasks = acknowledgeAndBuffer(acknowledgedSyncTasks)
		if (newlyAcknowledgedTasks > 0) {
			// do another loop
			runLoop(newlyAcknowledgedTasks)
		}
	}

	private val self = this
	private val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			val logId = Log.scope(self, "WorkLoop")
			// TODO: trampoline somewhere in here...
			// loop(1000)

			var numAcknowledged = acknowledgeAndBuffer(0)
			assert(numAcknowledged > 0) // shouldn't start loop unless tasks are ready
			runLoop(numAcknowledged)
		}
	}
}

object Capsul {
	val DEFAULT_LIMIT = 10

	/** Create a Capsul with the default bufLen */
	def apply[T](v: T)(implicit ec: ExecutionContext) = new Capsul[T](v, Capsul.DEFAULT_LIMIT)

	/** Create a Capsul with the provided bufLen */
	def apply[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) = new Capsul[T](v, bufLen)

	type State = Long
	type EnqueueableOp = EnqueueableTask with HasOpState
	val TASK_MASK = 0xFFFFFFFF // 8 lower bytes
	val FUTURE_SHIFT = 8
	val FUTURE_MASK = TASK_MASK << FUTURE_SHIFT // 8 top bytes
	val SINGLE_FUTURE = 0x01 << FUTURE_SHIFT // lower bit of FUTURE_MASK set
	def numQueued(state: State) = state & TASK_MASK
	def numFutures(state: State) = (state & FUTURE_MASK) >> FUTURE_SHIFT
	def size(state: State) = numFutures(state) + numQueued(state)

	// TODO: too many futures! how can we unify return type and work Promise?
	class Submit[T,R](capsul: Capsul[T], work: EnqueueableOp, ec: ExecutionContext) extends Op[Unit] {
		// A `Run` resolves once the item is submitted. Note: this op is non-retriable
		@tailrec final def attempt(promise: Promise[Unit]): Boolean = {
			val prevState = capsul.runState.get()
			// submit only runs loop if it is the first queued item (and there is capacity)
			if (numQueued(prevState) == 0 && numFutures(prevState) < capsul.limit) {
				if (capsul.runState.compareAndSet(prevState, prevState + 1)) {
					capsul.queue.add(work)
					promise.success(())
					true
				} else {
					// conflict, retry
					attempt(promise)
				}
			} else {
				false
			}
		}

		def perform(opState: Op.State, promise: Promise[Unit]) = {
			work.setOpState(opState)
			capsul.queue.add(work)
			val prevState = capsul.runState.getAndIncrement()
			val numQueued = Capsul.numQueued(prevState)
			val numFutures = Capsul.numFutures(prevState)
			if (numQueued == 0 && numFutures < capsul.limit) {
				ec.execute(capsul.workLoop)
			}

			if ((numQueued + numFutures) < capsul.limit) {
				work.enqueuedAsync()
				promise.success(())
			}
		}
	}

	trait HasOpState {
		private var _opState: Op.State = null // no need for volatile, always set before submitting work to the queue (plus a CAS on the state ref)

		def opState: Op.State = _opState
		def setOpState(newState: Op.State): Unit = {
			_opState = newState
		}
	}
}



/// Capsul API

import scala.concurrent.{ExecutionContext, Future, Promise}

/** A wrapper for getting / setting state */
class Ref[T](init:T) {
	private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}

/**
An encapsulation for thread-safe state.

Methods are named <dispatch><operation><mode>

== Dispatch types: ==

 - '''send''':   Enqueue an action, returning [[Future]][Unit]. Once the future is resolved, the
                 action has been accepted by the worker, but not necessarily performed.

 - '''(none)''': Perform an action, returning a [[StagedFuture]][T].

== Operations: ==

 - '''mutate''':    accepts a function of type `[[Ref]][T] => R`, returns `R`
 - '''transform''': accepts a function of type `T => T`, returns [[Unit]]
 - '''set''':       accepts a new value of type `T`, returns [[Unit]]
 - '''access''':    accepts a function of type `T => R`, returns `R`.
                    The function is executed sequentually with other tasks,
                    so it's safe to mutate `T`. If you simply want to get
                    the current vaue without blocking other tasks, use [[current]].

== Modes: ==

Modes control when the state will accept more work.
Each state will accept up to `bufLen` items immediately, and
will run the functions sequentually.

For asynchronous modes, subsequent tasks will still be run immediately
after the function completes, but the task won't be considered done
(for the purposes of accepting more tasks) until the future is resolved.

This means that you must be careful with `mutate` actions or with mutable
state objects - you may mutate the state during the execution of the
function, but you may not do so asynchronously (e.g. after your future
completes)

 - '''(empty)''': Synchronous. The task is completed as soon as it returns.
 - '''async''':   async ([[Future]]). The work returns a [[Future]][T], and the next
                  task may begin immediately. This task continues to occupy a
                  slot in this state's queue until the future completes.
 - '''staged''':  async ([[StagedFuture]]). The work returns a [[StagedFuture]][T], and
                  the next task may begin immediately. This task continues to
                  occupy a slot in this state's queue until the StagedFuture
                  is accepted.

*/
