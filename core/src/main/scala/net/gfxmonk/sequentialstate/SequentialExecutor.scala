package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import net.gfxmonk.sequentialstate.internal.Log

import monix.execution.atomic._

import scala.concurrent.{ExecutionContext, Future}


class RingItem[T>:Null<:AnyRef] {
	@volatile private var contents:T = null
	def set(item:T) {
		contents = item
	}

	def get: T = {
		contents
	}
}

/* Topography / terminology
 *
 * RingBuffer is based on https://www.snellman.net/blog/archive/2016-12-13-ring-buffers/
 * (except that we overflow at 2*size, rather than relying on size being a power of 2)
 *
 * If head == tail, the queue is empty
 * If (head + size) % (2*size) == tail, the queue is full
 * (these wrap to the same index mod `size`, but are on different sides / folds)
 *
 * The queue of an executoe is managed by:
 * state: (head|tail|numQueued) - bin packed to save on tuple allocation (we make and discard a _lot_ of these)
 * numFutures: Atomic[Int] (could do separate numFuturesStarted and numFuturesStopped, but have to take care of wraparound)
 * queue: a ConcurrentLinkedQueue for putting items which haven't yet been accepted into the ring buffer.
 *
 * To see if there's physically space available, compare head & tail. If there's space but it appears to be taken by numFutures, don't enqueue.
 * If we're running a loop of many futures, we could delay the future bookeeping until every `n` items.
 *
 * We can get away with not tying numFutures into state, because it's advisory. If we run ahead / behind a little bit, it doesn't matter because
 * it doesn't affect correctness.
 */
object Ring {
	type Count = Int
	type Idx = Int

	// State is stored as a uint64. head & tail indices both get 4 bytes, numQueued gets 8 bytes.
	val MAX_QUEUED = Math.pow(2, (8 * 4) - 1) // TODO: CHECK
	val MAX_SIZE = Math.pow(2, (8 * 2) - 1) // TODO: CHECK
	def queueSpaceExhausted(t: State): Boolean = numQueued(t) == MAX_QUEUED

	// if there's no work, we are (or should be) stopped
	def isStopped(t:State): Boolean = tailIndex(t) == headIndex(t)

	// just for testing / debugging
	def repr(t:State) = {
		(headIndex(t), tailIndex(t), numQueued(t))
	}




	// ------------------------------------------------
	// // # Simple implementation, for debugging
	// type State = (Int, Int, Int)
	// type AtomicState = AtomicAny[State]
	// def make(head: Idx, tail: Idx, numQueued: Count) = {
	// 	(head, tail, numQueued)
	// }
	// def headIndex(t:State):Idx = t._1
	// def tailIndex(t:State):Idx = t._2
	// def numQueued(t:State):Count = t._3
	// def incrementQueued(t:State): State = make(headIndex(t), tailIndex(t), numQueued(t) + 1)
	// ------------------------------------------------
	// # Packed implementation, for performance. Head(2)|Tail(2)|NumQueued(4)
	type State = Long
	type AtomicState = AtomicLong
	private val HEAD_OFFSET = 48 // 64 - 16
	private val TAIL_OFFSET = 32 // 64 - (2*16)
	private val IDX_MASK = 0xffff // 2 bytes (16 bits)
	private val QUEUED_MASK = 0xffffffff // 4 bytes (32 bits)
	def make(head: Idx, tail: Idx, numQueued: Count) = {
		(head.toLong << HEAD_OFFSET) | (tail.toLong << TAIL_OFFSET) | (numQueued.toLong)
	}
	def headIndex(t:State):Idx = (t >>> HEAD_OFFSET).toInt
	def tailIndex(t:State):Idx = ((t >>> TAIL_OFFSET) & IDX_MASK).toInt
	def numQueued(t:State):Count = (t & QUEUED_MASK).toInt
	def incrementQueued(t:State): State = t + 1 // since queued is the last 8 bytes, we can just increment the whole int
	// ------------------------------------------------

}

class Ring[T >: Null <: AnyRef](size: Int) {
	import Ring._
	if (size > MAX_SIZE) {
		throw new RuntimeException(s"size ($size) is larger then the maximum ($MAX_SIZE)")
	}

	private val bounds = size * 2
	private val negativeOne = bounds - 1
	private var arr = Array.fill(size)(new RingItem[T])

	def mask(idx: Idx): Idx = (idx % size)

	def at(idx: Idx): RingItem[T] = arr(mask(idx)) // unsafe woo

	def add(i: Idx, n: Int) = {
		// assumes `n` is never greater than `bounds`
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
	private val stateRef:Ring.AtomicState = Atomic(Ring.make(0,0,0))
	private val numFuturesRef:AtomicInt = Atomic(0)

	// don't advance until you've freed up this many slots
	private val advanceMinThreshold = Math.max(3, bufLen / 4)

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
		val logId = Log.scope(this, s"dequeueItemsInto($dest, $numItems)")
		var idx = dest
		var n = numItems
		while(n > 0) {
			var queued = queue.poll()
			while (queued == null) {
				// spinloop, since reserved (but un-populated) slots
				// in the ring will hold up the executor (and we
				// also run this from the executor)
				queued = queue.poll()
			}
			ring.at(idx).set(queued)
			queued.enqueuedAsync()
			idx = ring.inc(idx)
			n -= 1
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
		val logId = Log.scope(this, "dequeueIfSpace")
		val numQueued = Ring.numQueued(state)
		if (numQueued > 0) {
			val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get)
			if (spaceAvailable > 0) {
				// try inserting at `tail`
				// log(s"space may be available (${Ring.repr(state)})")
				val numQueued = Ring.numQueued(state)
				val numDequeue = Math.min(spaceAvailable, numQueued)
				val nextState = ring.dequeueAndReserve(state, numDequeue, 0)
				if (stateRef.compareAndSet(state, nextState)) {
					// We reserved all the slots we asked for, now assign into those slots
					log(s"reserved ${numDequeue} dequeue slots from ${Ring.tailIndex(state)}")
					val prevTail = Ring.tailIndex(state)
					val _:Idx = dequeueItemsInto(prevTail, numDequeue)
					startIfEmpty(state)
					numDequeue
				} else {
					// couldn't reserve tail; retry
					dequeueIfSpace(stateRef.get)
				}
			} else {
				0
			}
		} else {
			0
		}
	}

	@tailrec
	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val logId = Log.scope(this, "Executor.doEnqueue")
		val state = stateRef.get
		val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get)
		if (spaceAvailable > 0) {
			// try inserting at `tail`
			// log(s"space may be available (${Ring.repr(state)})")
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val numWork = if (spaceAvailable > numDequeue) 1 else 0
			val nextState = ring.dequeueAndReserve(state, numDequeue, numWork)
			if (stateRef.compareAndSet(state, nextState)) {
				log(s"reserved ${numDequeue} dequeue and ${numWork} slots from ${Ring.tailIndex(state)}")
				val prevTail = Ring.tailIndex(state)
				val nextIdx = dequeueItemsInto(prevTail, numDequeue)
				if (numWork == 0) {
					startIfEmpty(state)
					// gazumped by queued work; retry
					return doEnqueue(work)
				} else {
					log(s"inserting work into $nextIdx")
					ring.at(nextIdx).set(work)
					startIfEmpty(state)
					return true
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
				log(s"after putting item in overflow queue, state is (${Ring.repr(nextState)})")
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

	// A runnable which reepeatedly consumes & runs items in
	// the ring buffer until it's empty
	private val self = this
	val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			loop(1000)
		}

		@tailrec
		private def loop(_maxItems: Int): Unit = {
			val logId = Log.scope(self, "WorkLoop")
			if (_maxItems <= 0) {
				log("trampolining to prevent starvation")
				return ec.execute(workLoop)
			}

			var state = stateRef.get
			var currentHead = Ring.headIndex(state)
			var maxItems = _maxItems

			val currentTail = Ring.tailIndex(state)
			val readyItems = ring.numItems(currentHead, currentTail)

			// There must be something pending. Only this loop updates head, and
			// it terminates if head becomes empty
			// assert (!Ring.isStopped(state))

			log(s"workLoop: ${Ring.repr(state)}, head = $currentHead, tail = $currentTail")
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
					}
					case Some(f) => {
						log("ran async node")
						numFuturesRef.increment()
						// now that we've incremented running futures, set it up to decrement on completion
						f.onComplete { _ =>
							numFuturesRef.decrement()
							log(s"item completed asynchronously, there are now ${numFuturesRef.get} outstanding futures")
							// try to dequeue. If there's no space available then either someone
							// beat us to it, or there was a temporary burst of >bufLen async items
							dequeueIfSpace(stateRef.get)
						}
					}
				}

				slot.set(null) // must be nulled before advancing
				currentHead = ring.inc(currentHead)

				// Try updating state, but don't worry if it fails
				if (fullyCompletedItems > advanceMinThreshold) {
					state = advanceHeadTo(state, currentHead, fullyCompletedItems) match {
						case Some(newState) => {
							fullyCompletedItems = 0 // reset since these have been taken into account
							newState
						}
						case None => stateRef.get
					}
				}

				maxItems -= 1
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
				log(s"shutting down with state ${Ring.repr(state)} and ${numFuturesRef.get} pending futures")
				return
			} else {
				// more work arrived, kepp going
				return loop(maxItems)
			}
		}

		def advanceHeadTo(state: State, head: Idx, minimumReclaimedSpaces: Int): Option[State] = {

			// we can't just dequeue as many items as we're advancing head by, since some of them
			// may be "taken" by outstanding futures. So minimumReclaimedSpaces represents only
			// the synchronous items that have completed.
			val spaceAvailable = ring.spaceAvailable(state, numFuturesRef.get) + minimumReclaimedSpaces
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val tail = Ring.tailIndex(state)
			val newTail = if (numDequeue != 0) ring.add(tail, numDequeue) else tail
			val newState = Ring.make(head, newTail, numQueued - numDequeue)
			if (stateRef.compareAndSet(state, newState)) {
				dequeueItemsInto(Ring.tailIndex(state), numDequeue)
				Some(newState)
			} else {
				None
			}
		}
	}
}
