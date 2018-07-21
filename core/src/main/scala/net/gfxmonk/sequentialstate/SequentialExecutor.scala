package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import net.gfxmonk.sequentialstate.internal.Log

import monix.execution.atomic.{Atomic,AtomicAny,AtomicInt}

import scala.concurrent.{ExecutionContext, Future}

/*
 * Topography / terminology
 *
 * Head is the earliest piece of work added, and the
 * next piece of work which will be executed.
 * Tail is the firs free slot. If equal to head, the queue is empty.
 * If equal to (head-1), the queue is full. This means we have a spare
 * slot to distinguish empty from full.
 * Ready is the same as `tail`, but is updated after the tail
 * item has been set. If `ready` is != `tail`, then the slots in tail
 * have been reserved but are not yet guaranteed to be populated.
 *
 * Note that `ready` points one after the final ready item, for consistency with `tail`.
 *
 * Examples:
 *
 * Key:
 *  [x.]: work item
 *  [?.]: undetermied (may or may not be set
 *  [0.]: unset / garbage (should not access)
 *  [>.]: (possibly) incomplete future
 *
 *  [.h]: head (index)
 *  [.H]: head (index + runningFutures)
 *  [.R]: ready index
 *  [.T]: tail
 *
 * For a ring of 8, we have 9 slots:
 * [0,1,2,3,4,5,6,7,8]
 *
 * empty:
 * [0. 0hH 0RT 0. 0. ] (ready = tail = (head+1))
 *
 * full of work (but some not yet ready):
 * [x. ?R ?. 0T xhH x. ] (first available work = Head, last available work = ready-1)
 *
 * full of work (all ready)
 * [x. x. x. 0RT xhH x. ] (first available work = Head, last available work = ready-1)
 *
 * full, three incomplete futures
 * [x. 0RT >h >. >. xH ] (
 *
 *
 *
 *
 * OK new plan:
 * numFuturesStarted: volatile (only ever incremented by run thread)
 * numFuturesCompleted: Atomic[Int] (decremented by any completed future)
 * ^ Note: if we're happy to eat the serialization, we could make this volatile but only submit such events to a single-threaded pool
 * ^ both above wrap around at a certain value (2^^32), we know they can't get more than BUFLEN apart. numFuturesStarted should always be >= numFuturesCompleted,
 *   so if it's the other way around we have to add 2^^32 to it until numFuturesCompleted wraps around
 *
 * state: (head|tail|numQueued)
 *   - head & tail are both stored mod (bufsize*2). If head == tail then it's empy, but if (head + bufsize % twobuf) == tail then it's full
 *     (TODO: maybe another slot of memory is cheaper? Let's leave it for the initial impl). These can both have 2 bytes, numQueued would get 6 if it really wants it
 *     (TODO: block if numQueued wraps?)
 *
 * we also have `ready`, which is just volatile. It only gets incremented by the thread who reserved `tail`, so no need for CAS
 *
 * Option: also `headAdvisory`, which is updated every N items during a work batch. Saves us from doing an expensive CAS, and
 * quers are free to take it into account (and even pop it in HEAD for us). Have to be careful to check it doesn't get misleading
 * if it's not updated for a while. I think it should never lag behind by more than bufsize + N. But don't bother implementing
 * until later.
 *
 * To see if there's physically space available, compare head & tail. If there's space but it appears to be taken by (numFuturesStarted - numFuturesCompleted), don't enqueue.
 * This loop might be expensive, as it's 2 volatile reads and one CAS. Alternative is to bundle in numFutures into this state too,
 * but that gets contentious. Also we could only actually update the futures every `n` attempts (4?). Another alternative is just a single CAS counter,
 * then there's contention after spawning a future and when it completes.
 *
 * We can get away with not tying numFutures into state, because it's advisory. If we run ahead / behind a little bit, it doesn't matter because
 * it doesn't affect correctness.
 */

class RingItem[T>:Null<:AnyRef] {
	@volatile private var contents:T = null
	def set(item:T) { contents = item }
	def get: T = contents
}

object Ring {
	type Idx = Int
	type HeadIdx = Int
	type TailIdx = Int
	type QueueSize = Int
	type PromiseCount = Int
	type State = (HeadIdx, TailIdx, QueueSize)

	def tailIndex(t:State) = t._1
	def headIndex(t:State) = t._2
	def numQueued(t:State) = t._3
	// def hasQueuedWork(t:Tail) = numQueued(t) != 0
	def incrementQueued(t:State) = (headIndex(t), tailIndex(t), numQueued(t) + 1)
	// def decrementQueued(t:Tail) = (tailIndex(t), numQueued(t) - 1)

	def incrementRunningFutures(i:PromiseCount) = i+1
	def decrementRunningFutures(i:PromiseCount) = i-1
	// def withHeadIndex(t:Head, i: Idx) = (i, numRunningFutures(t))

	// if there's work, we are (or should be) running
	// TODO: invert to isStopped?
	def isRunning(t:State) = tailIndex(t) != headIndex(t)
	// def readyIndex(t:Ready) = t._1
	// def setRunning(t:Ready) = (t._1, true)
	// def setStopped(t:Ready) = (t._1, false)
	// def withIndex(i: Idx) = (i, true)
}

class Ring[T >: Null <: AnyRef](size: Int) {
	import Ring._
	// TODO: move these to executorState, for cleaner separation
	val queue = new ConcurrentLinkedQueue[T]()
	val stateRef:AtomicAny[State] = Atomic((0,0,0))
	val numFuturesRef:AtomicInt = Atomic(0)

	// we need the first inserted tail to advance `ready` to 0 (TODO: CHECK)
	// @volatile var readyIndex = negativeOne

	private val bounds = size * 2
	private val negativeOne = bounds - 1
	private var arr = Array.fill(size)(new RingItem[T])

	// RING
	def at(idx: Idx) = arr(idx % size) // unsafe woo
	def add(a: Idx, b: Int) = {
		(a + b) % bounds
	}
	def inc(a: Idx) = add(a, 1) // TODO specialize?
	// actually adding a negative is problematic due to `%` behaviour for negatives
	def dec(a: Idx) = add(a, negativeOne)

	def spaceBetween(head: Idx, tail: Idx):Int = {
		// queue is full when tail has wrapped around to one less than head
		val diff = tail - head
		if (diff < 0) {
			diff + size // there's never negative space available
		} else {
			diff
		}
	}

	def spaceAvailable(s: State):Int = {
		// XXX reuse `spaceBetween`
		// queue is full when tail has wrapped around to one less than head
		var diff = add(headIndex(s), size) - tailIndex(s)
		if (diff < 0) diff = diff + size // there's never negative space available

		if (diff > 0) diff = diff - numFuturesRef.get
		// Note: numFuturesRef is not synchronized with the rest of state; it may be inaccurate
		if (diff < 0) diff = 0
		return diff
	}

	def dequeueAndReserve(t:State, numDequeue: Int, numWork: Int) = {
		// dequeue up to numDequeue & reserve up to (numDequeue + numWork) ring slots
		val head = headIndex(t)
		val tail = tailIndex(t)
		(head, add(tail, numDequeue + numWork), numQueued(t) - numDequeue)
	}

	// def advanceHeadTo(t: State, newHead: Idx): (Int, State) = {
	// 	val spaceAvailable = this.spaceAvailable(t) + this.spaceBetween(Ring.headIndex(t), newHead)
	// 	val numQueued = Ring.numQueued(t)
	// 	val spaceUsed = Math.min(spaceAvailable, numQueued)
	// 	val tail = Ring.tailIndex(t)
	// 	val newTail = if (spaceUsed != 0) add(tail, spaceUsed) else tail
	// 	(newHead, newTail, numQueued - spaceUsed)
	// }
	// def headActionableIndex(t:Head) = add(t._1, t._2)
	// def addToHeadIndex(t:Head, n: Int) = (add(headIndex(t), n), numRunningFutures(t))
}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val ring = new Ring[EnqueueableTask](bufLen)
	import Log.log
	import Ring._

	def enqueueOnly[R](task: EnqueueableTask with UnitOfWork.HasEnqueuePromise[Unit]): Future[Unit] = {
		if (doEnqueue(task)) {
			SequentialExecutor.successfulUnit
		} else {
			task.enqueuedPromise.future
		}
	}

	def enqueue[R](
		task: EnqueueableTask
			with UnitOfWork.HasEnqueuePromise[Future[R]]
			with UnitOfWork.HasResultPromise[R]
	): StagedFuture[R] = {
		if (doEnqueue(task)) {
			StagedFuture.accepted(task.resultPromise.future)
		} else {
			StagedFuture(task.enqueuedPromise.future)
		}
	}

	// private def setReady(previousIndex: Idx, targetIndex: Idx) {
	// 	val logId = Log.scope("Executor.setReady")
	// 	var readyIndex = ring.readyIndex
	// 	while(readyIndex != previousIndex) {
	// 		// some other thread needs to advance `ready` before we can, give them a chance
	// 		// (TODO: should we spinlook a bit first?)
	// 		log(s"readyIdx = $readyIndex, awaiting $previousIndex")
	// 		LockSupport.parkNanos(1)
	// 		currentReady = ring.readyIndex
	// 	}
	// 	ring.readyIndex = targetIndex
	// }

	// private def insertQueuedWork(prev: State) = {
	// 	val logId = Log.scope("Executor.insertQueuedWork")
	// 	val tailIndex = Ring.tailIndex(prev)
	// 	// may block, but must complete because another thread has committed to queueing
	// 	val queued = ring.queue.take()
	// 	log(s"dequeued item $queued")
	// 	ring.at(tailIndex).set(queued)
	// 	setReady(tailIndex, isWorkLoop)
	// }

	// private def dequeueItem(): EnqueueableTask = {
	// 	val queued = ring.queue.wait() // XXX is this the api?
	// 	log(s"dequeued item $queued")
	// 	queued.enqueuedAsync()
	// 	queued
	// }

	private def dequeueItemsInto(dest: Idx, numItems: Int): Idx = {
		val logId = Log.scope("dequeueItemsInto")
		var idx = dest
		var n = numItems
		while(n > 0) {
			// XXX can we wait for all `n` at once? Iterator?
			var queued = ring.queue.poll()
			while (queued == null) {
				// spinloop, since reserved (but un-populated) slots
				// in the ring will hold up the executor (and we
				// also run this from the executor)
				queued = ring.queue.poll()
			}
			log(s"dequeued item $queued")
			ring.at(idx).set(queued)
			queued.enqueuedAsync()
			idx = ring.inc(dest)
		}
		return idx
	}

	private def startIfEmpty(state: State) {
		if (!Ring.isRunning(state)) {
			ec.execute(workLoop)
		}
	}

	// private def insertWork(prev: State, item: EnqueueableTask): Boolean = {
	// 	val logId = Log.scope("Executor.insertWork")
	// 	val tailIndex = Ring.tailIndex(prev)
	// 	ring.at(tailIndex).set(queued)
	// 	setReady(tailIndex, isWorkLoop)
	// 	startIfEmpty(prev)
	// }
  //
	// 	// required: lastTail is reserved
	// 	// returns: true if item was inserted, false if
	// 	// item was interrupted by queued work
	// 	val logId = Log.scope("Executor.insertWork")
	// 	val tailIndex = Ring.tailIndex(lastTail)
	// 	if (hasQueuedWork(lastTail)) {
	// 		val queued = ring.queue.poll()
	// 		log(s"dequeued item $queued")
	// 		if (queued != null) {
	// 			ring.at(tailIndex).set(queued)
	// 			setReady(currentTail, isWorkLoop)
	// 			ring.tailRef.transform(Ring.decrementQueued)
	// 			log("decremented quqeued items")
	// 			queued.enqueuedAsync()
	// 			return false
	// 		} // else someone already cleared out the queue
	// 	}
	// 	log(s"no queued items, inserting work")
	// 	ring.at(tailIndex).set(item)
	// 	setReady(currentTail, isWorkLoop)
	// 	return true
	// }

	@tailrec
	private def dequeueIfSpace(state: State): Int = {
		val logId = Log.scope("dequeueIfSpace")
		val numQueued = Ring.numQueued(state)
		if (numQueued > 0) {
			val spaceAvailable = ring.spaceAvailable(state)
			if (spaceAvailable > 0) {
				// try inserting at `tail`
				log(s"space may be available ($state)")
				val numQueued = Ring.numQueued(state)
				val numDequeue = Math.min(spaceAvailable, numQueued)
				val nextState = ring.dequeueAndReserve(state, numDequeue, 0)
				if (ring.stateRef.compareAndSet(state, nextState)) {
					// We reserved all the slots we asked for,
					// we need to set items then advance `ready` to our slot.
					log(s"reserved ${numDequeue} dequeue slots from ${Ring.tailIndex(state)}")
					val prevTail = Ring.tailIndex(state)
					val _:Idx = dequeueItemsInto(prevTail, numDequeue)
					// setReady(ring.add(prevTail, numDequeue))
					startIfEmpty(state)
					return numDequeue
				} else {
					// couldn't reserve tail; retry
					return dequeueIfSpace(ring.stateRef.get)
				}
			} else {
				return 0
			}
		} else {
			return 0
		}
	}

	@tailrec
	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val logId = Log.scope("Executor.doEnqueue")
		// first things first, is there spare capacity?
		val state = ring.stateRef.get
		val spaceAvailable = ring.spaceAvailable(state)
		if (spaceAvailable > 0) {
			// try inserting at `tail`
			log(s"space may be available ($state)")
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val numWork = if (spaceAvailable > numDequeue) 1 else 0
			val nextState = ring.dequeueAndReserve(state, numDequeue, numWork)
			if (ring.stateRef.compareAndSet(state, nextState)) {
				log(s"reserved ${numDequeue} dequeue and ${numWork} slots from ${Ring.tailIndex(state)}")
				val prevTail = Ring.tailIndex(state)
				val nextIdx = dequeueItemsInto(prevTail, numDequeue)
				if (numWork != 0) {
					ring.at(nextIdx).set(work)
					// setReady(prevTail, nextIdx)
					startIfEmpty(state)
					return true
				} else {
					// setReady(ring.add(prevTail, numDequeue))
					startIfEmpty(state)
					// gazumped by queued work; retry
					return doEnqueue(work)
				}
			} else {
				// couldn't reserve tail; retry
				return doEnqueue(work)
			}
		} else {
			val nextState = Ring.incrementQueued(state)
			if (ring.stateRef.compareAndSet(state, nextState)) {
				log(s"putting item in queue ($state)")
				ring.queue.add(work)
				if (!Ring.isRunning(nextState)) {
					// We've just added a queued item to a stopped state.
					// This can only mean that all slots are taken by futures
					//
					// To guard against futures completing and failing to enqueue
					// further work, we ensure that there's _still_ no space
					dequeueIfSpace(nextState)
				}
				return false
			} else {
				// CAS failed, try again
				return doEnqueue(work)
			}
		}
	}

	val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			Log("begin")
			loop(200)
		}

		def advanceHeadTo(state: State, head: Idx, minimumReclaimedSpaces: Int): Option[State] = {
			// TODO: don't use option?

			// we can't just dequeue as many items as we're advancing head by, since
			// some of them may be "taken" by outstanding futures.
			val spaceAvailable = ring.spaceAvailable(state) + minimumReclaimedSpaces
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val tail = Ring.tailIndex(state)
			val newTail = if (numDequeue != 0) ring.add(tail, numDequeue) else tail
			val newState = (head, newTail, numQueued - numDequeue)
			if (ring.stateRef.compareAndSet(state, newState)) {
				if (numDequeue > 0) {
					dequeueItemsInto(Ring.tailIndex(state), numDequeue)
				}
				Some(newState)
			} else {
				None
			}
		}


		@tailrec
		private def loop(_maxItems: Int): Unit = {
			if (_maxItems < 0) return ec.execute(workLoop)

			val logId = Log.scope("WorkLoop.loop")
			var state = ring.stateRef.get
			var currentHead = Ring.headIndex(state)
			var maxItems = _maxItems

			val currentTail = Ring.tailIndex(state)
			val readyItems = ring.spaceBetween(currentHead, currentTail)

			// There must be something pending. Only this loop updates head, and
			// it terminates if head becomes empty
			// TODO should we yield or trampoline?
			assert (Ring.isRunning(state))

			log(s"workLoop: $state, head = $currentHead, tail = $currentTail")
			var fullyCompletedItems = 0
			while (currentHead != currentTail) {
				log(s"executing item @ $currentHead")
				val slot = ring.at(currentHead)
				var item = slot.get
				while(item == null) {
					// spin waiting for item to be set
					item = slot.get
				}

				item.run().filter(!_.isCompleted) match {
					case None => {
						log("ran sync node")
						fullyCompletedItems = fullyCompletedItems + 1
						// TODO: we don't have to acknowledge every sync item, we can do it in a batch to save CAS operations.
						// But don't let HEAD lag behind reality more than `bufLen / 2`
						// unacknowledgedCompletions += 1
						// if (unacknowledgedCompletions >= bufLen / 2) {
						// 	if(ring.headRef.compareAndSet(currentHead, Ring.withHeadIndex(currentHead, index))) {
						// 		currentHead = ring.headRef.get
						// 	} else {
						// 		unacknowledgedCompletions = 0
						// 	}
						// }
					}
					case Some(f) => {
						log("ran async node")
						ring.numFuturesRef.transform(n => Ring.incrementRunningFutures(n))
						// now that we've incremented running futures, set it up to decrement on completion
						f.onComplete { _ =>
							ring.numFuturesRef.transform(n => Ring.decrementRunningFutures(n))
							// try to dequeue. If there's no space available then either someone
							// beat us to it, or there was a temporary burst of >bufLen async items
							dequeueIfSpace(ring.stateRef.get)

							// TODO: ensure workLoop is running, dequeue work?
							// worst case:
							//  - ring capacity is 1
							//  - there is an incomplete future
							//
							// enqueue decides to inc queued from 0 -> 1
							//
							// future completes, decrements count by one
							//
							// if future checks state now, it's still empty (stopped)
							//
							// enqueue _succeeds_ setting new state (queued = 1)
							//
							// if enqueue checks futures now, it _would_ see that there's space available
							// (there can't be more futures started in the meantime because
							//
						}
					}
				}

				currentHead = ring.inc(currentHead)

				// try updating state, but don't worry if it fails
				state = advanceHeadTo(state, currentHead, fullyCompletedItems) match {
					case Some(newState) => {
						fullyCompletedItems = 0 // reset since these have been taken into account
						newState
					}
					case None => ring.stateRef.get
				}
				slot.set(null) // aid GC; if we keep this we could remove `readyIndex`
			}

			// make sure we've definitely updated head:
			while (Ring.headIndex(state) != currentHead) {
				state = advanceHeadTo(state, currentHead, fullyCompletedItems) match {
					case Some(newState) => newState
					case None => ring.stateRef.get
				}
			}

			// state _must_ have been set to the most recent post-CAS result,
			// so we can now use it to determine whether to stop
			if (!Ring.isRunning(state)) {
				log("shutting down")
				return
			} else {
				// there's more work coming, just continue
				return loop(maxItems)
			}
		}
	}
}

		// @tailrec
		// private def shouldKeepRunningAfterBatch(currentHead: Head): Boolean = {
		// 	val logId = Log.scope("WorkLoop.shouldKeepRunningAfterBatch")
		// 	val currentReady = ring.readyRef.get
		// 	if (Ring.readyIndex(currentReady) == ring.headActionableIndex(currentHead)) {
		// 		// no more work. Is there queued work though?
		// 		val currentTail = ring.tailRef.get
		// 		if (hasQueuedWork(currentTail) && ring.spaceAvailable(currentHead, currentTail)) {
		// 			// try reserving a tail slot
		// 			val nextTail = ring.nextTail(currentTail)
		// 			if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
		// 				log(s"attempting to dequeue work into slot $currentTail")
		// 				insertWork(currentTail, nextTail, UnitOfWork.noop, true)
		// 				return true // either we dequeued work, or we were beaten to it
		// 				// TODO is it possible for this to spin, if the thread which was decrementing
		// 				// the tail queue count is interrupted?
		// 			} else {
		// 				// couldn't reserve slot, retry
		// 				return shouldKeepRunningAfterBatch(currentHead)
		// 			}
		// 		} else {
		// 			// no queued work, or no space avilable
		// 			log(s"no queued work, or no space available")
		// 			if (ring.readyRef.compareAndSet(currentReady, setStopped(currentReady))) {
		// 				log("stopping")
		// 				return false
		// 			} else {
		// 				// `ready` changed, loop again
		// 				log("stop request failed, trying again")
		// 				return shouldKeepRunningAfterBatch(currentHead)
		// 			}
		// 		}
		// 	} else {
		// 		// ready is less than head, we've already got work to do
		// 		log("more work detected")
		// 		return true
		// 	}
		// }

	// private def didEnqueueSingleItem() {
	// 	// either we inserted an item but couldn't mark the `tail` as having queued work,
	// 	// or we inserted an item somewhere at the end of the queue.
	// 	val currentTail = ring.tailRef.get
	// 	if (hasQueuedWork(currentTail)) {
	// 		// OK, we know there's queued work already
	// 		return
	// 	} else {
	// 		// space just cleared up. Either the ring is _precisely_ full, or there's now room.
	// 		val currentHead = head.get
	// 		if (spaceAvailable(currentHead, currentTail) {
	// 			// try reserving a slot
	// 			val nextTail = currentTail + 1
	// 			if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
	// 				// OK, we have reserved the slot at `currentTail`.
	// 				// pop the work we just added on tail instead
	// 				item = queue.pop()
	// 				if (item == null) {
	// 					// Someone already dequeued our item. We can't give back our slot, but we can set it to a
	// 					// noop
	// 					return
	// 				} else {
	// 					ring.at(currentTail).set(item)
	// 					setReady(currentTail)
	// 				}
	// 			} else {
	// 				// couldn't get that tail slot, just loop and try again
	// 				didEnqueueSingleItem()
	// 			}
	// 		} else {
	// 			// no space available, try setting the queued flag
	// 			if (ring.tailRef.compareAndSet(currentTail, markEnqueued(currentTail))) {
	// 				return
	// 			} else {
	// 				didEnqueueSingleItem()
	// 			}
	// 		}
	// 	}
	// }

