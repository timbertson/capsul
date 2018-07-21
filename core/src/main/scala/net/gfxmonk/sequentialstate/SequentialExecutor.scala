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

	// TODO: state could be more efficiently stored as a packed (4,4,8) Long,
	// as long as I can figure out signed operations. We'll keep this
	// commented-out because it's much easier to debug
	type State = (HeadIdx, TailIdx, QueueSize)
	def make(head: HeadIdx, tail: TailIdx, numQueued: QueueSize) = {
		(head, tail, numQueued)
	}
	def headIndex(t:State) = t._1
	def tailIndex(t:State) = t._2
	def numQueued(t:State) = t._3

	// State is stored as a uint64. head & tail indices both get 4 bytes, numQueued gets 8 bytes.
	// I don't trust unsigned maths, so subtract one bit for safety :shrug:
	val MAX_QUEUED = Math.pow(2, (8 * 4) - 1) // TODO: CHECK
	val MAX_SIZE = Math.pow(2, (8 * 2) - 1) // TODO: CHECK

	def queueSpaceExhausted(t: State) = numQueued(t) == MAX_QUEUED
	def incrementQueued(t:State) = make(headIndex(t), tailIndex(t), numQueued(t) + 1)

	// if there's no work, we are (or should be) stopped
	def isStopped(t:State) = tailIndex(t) == headIndex(t)
	def incrementRunningFutures(i:PromiseCount) = i+1
	def decrementRunningFutures(i:PromiseCount) = i-1
}

class Ring[T >: Null <: AnyRef](size: Int) {
	import Ring._
	if (size > MAX_SIZE) {
		throw new RuntimeException(s"size ($size) is larger then the maximum ($MAX_SIZE)")
	}
	// we need the first inserted tail to advance `ready` to 0 (TODO: CHECK)
	// @volatile var readyIndex = negativeOne

	private val bounds = size * 2
	private val negativeOne = bounds - 1
	private var arr = Array.fill(size)(new RingItem[T])

	// RING
	def mask(idx: Idx) = (idx % size)
	def at(idx: Idx) = arr(mask(idx)) // unsafe woo
	def add(i: Idx, n: Int) = {
		// assumes `n` is never greater than `size`
		val result = i + n
		if (result >= bounds) {
			result - bounds
		} else {
			result
		}
	}
	def inc(a: Idx) = {
		// assuming this is more efficient than add(a,1)
		if (a == negativeOne) 0 else a + 1
	}
	// actually adding a negative is problematic due to `%` behaviour for negatives
	def dec(a: Idx) = add(a, negativeOne)

	def numItems(head: Idx, tail: Idx):Int = {
		// queue is full when tail has wrapped around to one less than head
		val diff = tail - head
		if (diff < 0) {
			diff + bounds // there's never negative space available, wrap around
		} else {
			diff
		}
	}

	def spaceAvailable(s: State, numFutures: Int):Int = {
		val space = size - numItems(Ring.headIndex(s), Ring.tailIndex(s)) - numFutures
		// Note: numFuturesRef is not synchronized with the rest of state; it may be inaccurate
		// (causing a negative space value)
		if (space < 0) {
			0
		} else {
			space
		}
	}

	def dequeueAndReserve(t:State, numDequeue: Int, numWork: Int): State = {
		// dequeue up to numDequeue & reserve up to (numDequeue + numWork) ring slots
		val head = headIndex(t)
		val tail = tailIndex(t)
		make(head, add(tail, numDequeue + numWork), numQueued(t) - numDequeue)
	}
}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val ring = new Ring[EnqueueableTask](bufLen)
	private val queue = new ConcurrentLinkedQueue[EnqueueableTask]()
	private val stateRef:AtomicAny[Ring.State] = Atomic((0,0,0))
	private val numFuturesRef:AtomicInt = Atomic(0)

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

	private def dequeueItemsInto(dest: Idx, numItems: Int): Idx = {
		val logId = Log.scope("dequeueItemsInto")
		var idx = dest
		var n = numItems
		while(n > 0) {
			// XXX can we wait for all `n` at once? Iterator?
			var queued = queue.poll()
			while (queued == null) {
				// spinloop, since reserved (but un-populated) slots
				// in the ring will hold up the executor (and we
				// also run this from the executor)
				queued = queue.poll()
			}
			log(s"dequeued item $queued")
			ring.at(idx).set(queued)
			queued.enqueuedAsync()
			idx = ring.inc(dest)
		}
		return idx
	}

	private def startIfEmpty(state: State) {
		if (Ring.isStopped(state)) {
			ec.execute(workLoop)
		}
	}

	@tailrec
	private def dequeueIfSpace(state: State): Int = {
		val logId = Log.scope("dequeueIfSpace")
		val numQueued = Ring.numQueued(state)
		if (numQueued > 0) {
			val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get)
			if (spaceAvailable > 0) {
				// try inserting at `tail`
				log(s"space may be available ($state)")
				val numQueued = Ring.numQueued(state)
				val numDequeue = Math.min(spaceAvailable, numQueued)
				val nextState = ring.dequeueAndReserve(state, numDequeue, 0)
				if (stateRef.compareAndSet(state, nextState)) {
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
					return dequeueIfSpace(stateRef.get)
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
		val state = stateRef.get
		val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get)
		if (spaceAvailable > 0) {
			// try inserting at `tail`
			log(s"space may be available ($state)")
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val numWork = if (spaceAvailable > numDequeue) 1 else 0
			val nextState = ring.dequeueAndReserve(state, numDequeue, numWork)
			if (stateRef.compareAndSet(state, nextState)) {
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
			if (Ring.queueSpaceExhausted(state)) {
				// I can't imagine this happening, but an exception seems
				// better than corruption due to integer overflow.
				throw new RuntimeException(
					s"Overflow detected - ${Ring.MAX_QUEUED} items in a single SequentialState queue"
				)
			}
			val nextState = Ring.incrementQueued(state)
			if (stateRef.compareAndSet(state, nextState)) {
				log(s"putting item in queue ($state)")
				queue.add(work)
				if (Ring.isStopped(nextState)) {
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
			val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get) + minimumReclaimedSpaces
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val tail = Ring.tailIndex(state)
			val newTail = if (numDequeue != 0) ring.add(tail, numDequeue) else tail
			val newState = (head, newTail, numQueued - numDequeue)
			if (stateRef.compareAndSet(state, newState)) {
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
			var state = stateRef.get
			var currentHead = Ring.headIndex(state)
			var maxItems = _maxItems

			val currentTail = Ring.tailIndex(state)
			val readyItems = ring.numItems(currentHead, currentTail)

			// There must be something pending. Only this loop updates head, and
			// it terminates if head becomes empty
			// TODO should we yield or trampoline?
			assert (!Ring.isStopped(state))

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
					}
					case Some(f) => {
						log("ran async node")
						numFuturesRef.transform(n => Ring.incrementRunningFutures(n))
						// now that we've incremented running futures, set it up to decrement on completion
						f.onComplete { _ =>
							numFuturesRef.transform(n => Ring.decrementRunningFutures(n))
							// try to dequeue. If there's no space available then either someone
							// beat us to it, or there was a temporary burst of >bufLen async items
							dequeueIfSpace(stateRef.get)

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
					case None => stateRef.get
				}
				slot.set(null) // aid GC; if we keep this we could remove `readyIndex`
			}

			// make sure we've definitely updated head:
			while (Ring.headIndex(state) != currentHead) {
				state = advanceHeadTo(state, currentHead, fullyCompletedItems) match {
					case Some(newState) => newState
					case None => stateRef.get
				}
			}

			// state _must_ have been set to the most recent post-CAS result,
			// so we can now use it to determine whether to stop
			if (Ring.isStopped(state)) {
				log("shutting down")
				return
			} else {
				// there's more work coming, just continue
				return loop(maxItems)
			}
		}
	}
}
