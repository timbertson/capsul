package net.gfxmonk.capsul.cml

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import net.gfxmonk.capsul.internal.Log
import net.gfxmonk.capsul.StagedFuture
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import scala.concurrent.{ExecutionContext, Future}

import UnitOfWork._

class Capsul[T](initial:T, val limit: Int)(implicit ec: ExecutionContext) {
	import Capsul._

	private val state = new Ref[T](initial)
	private val queue = new ConcurrentLinkedQueue[EnqueueableOp[_]]()

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
	def enqueueOnly(task: EnqueueableOp[Unit]) = {
		if (Capsul.Send.perform(this, task)) {
			Capsul.successfulUnit
		} else {
			task.enqueuedPromise.future
		}
	}

	def enqueue[T](task: EnqueueableOp[Future[T]] with HasResultPromise[T]): StagedFuture[T] = {
		if (Capsul.Send.perform(this, task)) {
			StagedFuture.accepted(task.resultPromise.future)
		} else {
			StagedFuture(task.enqueuedPromise.future)
		}
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

	def send[R](task: EnqueueableOp[R]): Op[R] = {
		new Send(this, task)
	}

	// Note: doesn't need to be volatile, since we
	// always write to it before reading in the same thread
	private var buffer = new Array[EnqueueableOp[_]](limit)

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

	@tailrec private def shouldDoWork(task: EnqueueableOp[_]): Boolean = {
		// TODO: is it really that useful to have Capsul participate in CML?
		import Op._
		var opState = task.opState.get()
		if (opState == WAITING) {
			if (task.opState.compareAndSet(WAITING, DONE)) {
				true
			} else {
				// CAS fail
				shouldDoWork(task)
			}
		} else if (opState == CLAIMED) {
			// spin, it's temporary
			shouldDoWork(task)
		} else {
			assert(opState == DONE)
			false
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
			
			var task = buffer(taskIdx)
			taskIdx += 1

			val resultFuture = if (task.opState == null || shouldDoWork(task)) {
				task.run().filter(!_.isCompleted)
			} else {
				None
			}
			resultFuture match {
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
	private val successfulUnit = Future.successful(())

	/** Create a Capsul with the default bufLen */
	def apply[T](v: T)(implicit ec: ExecutionContext) = new Capsul[T](v, Capsul.DEFAULT_LIMIT)

	/** Create a Capsul with the provided bufLen */
	def apply[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) = new Capsul[T](v, bufLen)

	type State = Long
	type EnqueueableOp[T] = EnqueueableTask with HasOpState with HasEnqueuePromise[T]
	val TASK_MASK = 0xFFFFFFFF // 8 lower bytes
	val FUTURE_SHIFT = 8
	val FUTURE_MASK = TASK_MASK << FUTURE_SHIFT // 8 top bytes
	val SINGLE_FUTURE = 0x01 << FUTURE_SHIFT // lower bit of FUTURE_MASK set
	def numQueued(state: State) = state & TASK_MASK
	def numFutures(state: State) = (state & FUTURE_MASK) >> FUTURE_SHIFT
	def size(state: State) = numFutures(state) + numQueued(state)

	object Send {
		def perform[T,R](capsul: Capsul[T], task: EnqueueableOp[R])(implicit ec: ExecutionContext): Boolean = {
			capsul.queue.add(task)
			val prevState = capsul.runState.getAndIncrement()
			val numQueued = Capsul.numQueued(prevState)
			val numFutures = Capsul.numFutures(prevState)
			if ((numQueued + numFutures) < capsul.limit) {
				if (numQueued == 0) {
					ec.execute(capsul.workLoop)
				}
				true
			} else {
				false
			}
		}
	}

	class Send[T,R](capsul: Capsul[T], task: EnqueueableOp[R])(implicit ec: ExecutionContext) extends Op[R] {
		// A `Send` resolves once the item is submitted, although cancellation affects the actual run (never submission). Note: this op is non-retriable
		@tailrec final def attempt(): Option[R] = {
			val prevState = capsul.runState.get()
			// Send only runs loop if it is the first queued item (and there is capacity)
			val numQueued = Capsul.numQueued(prevState)
			val numFutures = Capsul.numFutures(prevState)
			if (numQueued + numFutures < capsul.limit) {
				if (capsul.runState.compareAndSet(prevState, prevState + 1)) {
					capsul.queue.add(task)
					task.enqueuedAsync()

					if (numQueued == 0) {
						ec.execute(capsul.workLoop)
					}
					Some(task.enqueuedPromise.future.value.get.get) // TODO: surely this could be cleaner?
				} else {
					// conflict, retry
					attempt()
				}
			} else {
				// full, sorry!
				None
			}
		}

		final def perform(opState: Op.State, promise: Promise[R]) = {
			task.setOpState(opState)
			Send.perform(capsul, task)
			// XXX this promise succeeds on enqueue, which doesn't respect cancellation
			task.enqueuedPromise.future.foreach(promise.success(_))
		}
	}

	// TODO: we could conceptually have an Await op too, is it useful?

	trait HasOpState {
		private var _opState: Op.State = null // no need for volatile, always set before submitting work to the queue (plus a CAS on the state ref)

		def opState: Op.State = _opState
		def setOpState(newState: Op.State): Unit = {
			_opState = newState
		}
	}
}



import scala.concurrent.{ExecutionContext, Future, Promise}

/** A wrapper for getting / setting state */
class Ref[T](init:T) {
	private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}
