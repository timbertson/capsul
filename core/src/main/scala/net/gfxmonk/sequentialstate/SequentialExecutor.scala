package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import net.gfxmonk.sequentialstate.internal.Log
import net.gfxmonk.sequentialstate.internal.Log.log

import monix.execution.atomic._

import scala.concurrent.{ExecutionContext, Future}


class RingItem[T>:Null<:AnyRef] {
	@volatile private var contents:T = null
	@volatile private var idx:Int = 0

	def set(item:T, idx: Int) {
		// val logId = Log.scope(this, "slot")
		// log(s"set($item)")
		this.idx = idx
		contents = item
	}
	
	def clear() {
		contents = null
	}

	def get(idx: Int): T = {
		val ret = contents
		val lastIdx = this.idx
		if (ret != null && idx != lastIdx) {
			Log.dump()
			throw new RuntimeException(s"tried to get($idx) but last set was for ${lastIdx}")
		}
		// log(s"get = $ret")
		ret
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


/*
 * Idea:
 *
 * Split head & tail.
 *
 * head
 * tail | running | numQueued
 *
 * head being unhinged from tail is probably fine.
 * tail & isRunning must be tied, since otherwise we can't be sure we're running after inserting work.
 * only work advances head, and to do so it pops numQueued / advances tail. So this requires 2 CASes.
 *
 */

object Ring {
	type Count = Int
	type Idx = Int

	// State is stored as a uint64. head & tail indices both get 4 bytes, numQueued gets 8 bytes.
	// val MAX_QUEUED = 6000 // Math.pow(2, (8 * 4) - 1) // TODO: CHECK
	val MAX_QUEUED = Math.pow(2, (8 * 4) - 1) // TODO: CHECK
	val MAX_SIZE = Math.pow(2, (8 * 2) - 1) // TODO: CHECK
	def queueSpaceExhausted(t: State): Boolean = numQueued(t) == MAX_QUEUED

	// just for testing / debugging
	def repr(t:State) = {
		s"(isRunning=${!isStopped(t)}, tail=${tailIndex(t)}, queued=${numQueued(t)})"
	}
	def tuple(t:State) = {
		(!isStopped(t), tailIndex(t), numQueued(t))
	}




	// ------------------------------------------------
	// # Simple implementation, for debugging
	type Head = Int
	type State = (Boolean, Int, Int)
	type AtomicState = AtomicAny[State]
	def make(running: Boolean, tail: Idx, numQueued: Count) = {
		(running, tail, numQueued)
	}

	def isStopped(t:State): Boolean = !t._1
	def tailIndex(t:State):Idx = t._2
	def numQueued(t:State):Count = t._3
	def incrementQueued(t:State): State = make(!isStopped(t), tailIndex(t), numQueued(t) + 1)
	// ------------------------------------------------
	// # Packed implementation, for performance. Head(2)|Tail(2)|NumQueued(4)
	// type State = Long
	// type AtomicState = AtomicLong
	// private val HEAD_OFFSET = 48 // 64 - 16
	// private val TAIL_OFFSET = 32 // 64 - (2*16)
	// private val IDX_MASK = 0xffff // 2 bytes (16 bits)
	// private val QUEUED_MASK = 0xffffffff // 4 bytes (32 bits)
	// def make(head: Idx, tail: Idx, numQueued: Count) = {
	// 	(head.toLong << HEAD_OFFSET) | (tail.toLong << TAIL_OFFSET) | (numQueued.toLong)
	// }
	// def headIndex(t:State):Idx = (t >>> HEAD_OFFSET).toInt
	// def tailIndex(t:State):Idx = ((t >>> TAIL_OFFSET) & IDX_MASK).toInt
	// def numQueued(t:State):Count = (t & QUEUED_MASK).toInt
	// def incrementQueued(t:State): State = t + 1 // since queued is the last 8 bytes, we can just increment the whole int
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

	def spaceAvailable(s: State, head: Int, numFutures: Int):Int = {
		val space = size - numItems(head, Ring.tailIndex(s)) - numFutures
		// Note: numFuturesRef and head are both not synchronized with the rest of state;
		// so we may have a skewed view.
		// This is used to CAS on tail, so as long as head
		// is behind, this is a safe estimate (it can only be under, not over).
		if (space < 0) {
			0
		} else {
			space
		}
	}

	def dequeueAndReserve(t:State, numDequeue: Int, numWork: Int): State = {
		// dequeue up to numDequeue & reserve up to (numDequeue + numWork) ring slots
		val tail = tailIndex(t)
		make(true, add(tail, numDequeue + numWork), numQueued(t) - numDequeue)
	}
}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private [sequentialstate] val headRef:AtomicInt = Atomic(0)
	private [sequentialstate] val stateRef:Ring.AtomicState = Atomic(Ring.make(false,0,0))
	private [sequentialstate] val numFuturesRef:AtomicInt = Atomic(0)
	private val ring = new Ring[EnqueueableTask](bufLen)
	private val queue = new ConcurrentLinkedQueue[EnqueueableTask]()

	// don't advance until you've freed up this many slots
	private val advanceMinThreshold = Math.max(3, bufLen / 4)

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
			ring.at(idx).set(queued, idx)
			queued.enqueuedAsync()
			idx = ring.inc(idx)
			n -= 1
		}
		log(s"dequeued $numItems items into $dest (up to $idx)")
		return idx
	}

	private def startIfStopped(prevState: State) {
		// val logId = Log.scope(this, "startIfStopped")
		if (Ring.isStopped(prevState)) {
			// log(s"starting workLoop with previous state: $prevState")
			ec.execute(workLoop)
		}
	}

	@tailrec
	private def dequeueIfSpace(state: State, head: Idx) {
		// NOTE: may not completely fill ring even when there
		// are many items queued, since `head` may advance
		// during execution
		val logId = Log.scope(this, "dequeueIfSpace")
		val numQueued = Ring.numQueued(state)
		if (numQueued > 0) {
			val spaceAvailable = ring.spaceAvailable(state, head, numFuturesRef.get)
			if (spaceAvailable > 0) {
				// try inserting at `tail`
				// log(s"space may be available ($head, ${Ring.repr(state)})")
				val numDequeue = Math.min(spaceAvailable, numQueued)
				val nextState = ring.dequeueAndReserve(state, numDequeue, 0)
				if (stateRef.compareAndSet(state, nextState)) {
					// We reserved all the slots we asked for, now assign into those slots
					log(s"reserved ${numDequeue} dequeue slots from ${Ring.tailIndex(state)}")
					val prevTail = Ring.tailIndex(state)
					val _:Idx = dequeueItemsInto(prevTail, numDequeue)
					startIfStopped(state)
				} else {
					// couldn't reserve tail; retry
					// TODO should we re-fetch head first?
					dequeueIfSpace(stateRef.get, head)
				}
			}
		}
	}

	@tailrec
	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val logId = Log.scope(this, "Executor.doEnqueue")
		val state = stateRef.get
		val storedNumFutures = numFuturesRef.get
		val head = headRef.get
		val spaceAvailable = ring.spaceAvailable(state, head, storedNumFutures)
		if (spaceAvailable > 0) {
			// try inserting at `tail`
			// log(s"$spaceAvailable slots may may be available ($head, ${Ring.repr(state)}, $storedNumFutures)")
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val numWork = if (spaceAvailable > numDequeue) 1 else 0
			val nextState = ring.dequeueAndReserve(state, numDequeue, numWork)
			if (stateRef.compareAndSet(state, nextState)) {
				log(s"reserved ${numDequeue} dequeue and ${numWork} slots from ${Ring.tailIndex(state)}")
				val prevTail = Ring.tailIndex(state)
				val nextIdx = dequeueItemsInto(prevTail, numDequeue)
				if (numWork == 0) {
					startIfStopped(state)
					// gazumped by queued work; retry
					return doEnqueue(work)
				} else {
					log(s"inserting work into $nextIdx")
					ring.at(nextIdx).set(work, nextIdx)
					startIfStopped(state)
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
				log(s"after putting item in overflow queue, state is ($head, ${Ring.repr(nextState)})")
				queue.add(work)
				if (Ring.isStopped(state)) {
					// We've just added a queued item to a stopped state.
					// This must mean that all slots are taken by futures.
					//
					// To guard against futures completing and failing to enqueue
					// further work, we ensure that there's _still_ no space.
					dequeueIfSpace(nextState, headRef.get)
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
				return ec.execute(workLoop)
			}

			var state = stateRef.get
			var currentHead = headRef.get
			var storedHead = currentHead
			var maxItems = _maxItems

			val currentTail = Ring.tailIndex(state)
			var storedNumFutures = -1

			// There must be something pending. Only this loop updates head, and
			// it terminates once head catches up to tail
			// assert (!Ring.isStopped(state))

			log(s"workLoop[$maxItems]: ($currentHead, ${Ring.repr(state)}), head = $currentHead, tail = $currentTail")
			var fullyCompletedItems = 0
			while (currentHead != currentTail) {
				log(s"executing item @ $currentHead")
				val slot = ring.at(currentHead)
				var item = slot.get(currentHead)

				// var maxSpins = 500 // XXX remove
				// while(item == null && maxSpins > 0) {
				// 	// spin waiting for item to be set
				// 	log(s"spin #$maxSpins for $currentHead -- $item -- $slot....")
				// 	maxSpins -= 1
				// 	item = slot.get
				// }
				// if(item == null) throw new RuntimeException("maxSpins expired!")

				while(item == null) {
					// spin waiting for item to be set
					item = slot.get(currentHead)
				}

				item.run().filter(!_.isCompleted) match {
					case None => {
						log("ran sync node")
						fullyCompletedItems = fullyCompletedItems + 1
					}
					case Some(f) => {
						log("ran async node")
						storedNumFutures = numFuturesRef.incrementAndGet()
						// now that we've incremented running futures, set it up to decrement on completion
						f.onComplete { _ =>
							numFuturesRef.decrement()
							log(s"item completed asynchronously, there are now ${numFuturesRef.get} outstanding futures")
							// try to dequeue. If there's no space available then either someone
							// beat us to it, or there was a temporary burst of >bufLen async items
							dequeueIfSpace(stateRef.get, headRef.get)
						}
					}
				}

				// log(s"setting slot @ $currentHead to null")
				slot.clear() // must be nulled before advancing head
				currentHead = ring.inc(currentHead)

				if (fullyCompletedItems > advanceMinThreshold) {
					advanceHeadTo(currentHead)
					storedHead = currentHead
					state = stateRef.get
					if (storedNumFutures < 0) { // lame sentinel, but awkward to use Option
						storedNumFutures = numFuturesRef.get
					}
					state = optimisticallyDequeue(state, currentHead, storedNumFutures) match {
						case Some(newState) => newState
						case None => state
					}
					fullyCompletedItems = 0
				}

				maxItems -= 1
			}

			// after a loop, dequeue at least one item (but hopefully more)
			if(storedHead != currentHead) {
				advanceHeadTo(currentHead)
				storedHead = currentHead
			}
			state = dequeueOrStop(state, storedHead)

			// state _must_ have been set to the most recent post-CAS result,
			// so we can now use it to determine whether to stop
			if (Ring.isStopped(state)) {
				log(s"shutting down with state ($currentHead, ${Ring.repr(state)}) and ${numFuturesRef.get} pending futures")
				return
			} else {
				// more work arrived, kepp going
				return loop(maxItems)
			}
		}

		private def advanceHeadTo(newHead: Idx) {
			headRef.set(newHead)
			log(s"set head to new value $newHead")
		}

		private def optimisticallyDequeue(state: State, head: Idx, numFutures: Int): Option[State] = {
			val logId = Log.scope(self, "tryDequeueAfterHeadAdvance")
			val spaceAvailable = ring.spaceAvailable(state, head, numFutures)
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)

			if (numDequeue == 0) {
				// don't bother trying to stop `state`, we're only optimistically dequeueing
				None
			} else {
				val tail = Ring.tailIndex(state)
				val newTail = ring.add(tail, numDequeue)
				val newState = Ring.make(true, newTail, numQueued - numDequeue)
				if (stateRef.compareAndSet(state, newState)) {
					dequeueItemsInto(Ring.tailIndex(state), numDequeue)
					Some(newState)
				} else {
					None
				}
			}
		}

		@tailrec
		private def dequeueOrStop(state: State, head: Idx): State = {
			tryDequeueOrStop(state, head) match {
				case Some(newState) => newState

				// called from workLoop, so we know `head` won't change
				case None => dequeueOrStop(stateRef.get, head)
			}
		}

		private def tryDequeueOrStop(state: State, head: Idx): Option[State] = {
			val logId = Log.scope(self, "tryDequeueAfterWorkLoop")
			val spaceAvailable = ring.spaceAvailable(state, head, numFuturesRef.get)
			val numQueued = Ring.numQueued(state)
			val numDequeue = Math.min(spaceAvailable, numQueued)
			val tail = Ring.tailIndex(state)

			val newTail = if (numDequeue == 0) tail else ring.add(tail, numDequeue)
			val newRunning = newTail != head
			val newState = Ring.make(newRunning, newTail, numQueued - numDequeue)
			if (stateRef.compareAndSet(state, newState)) {
				dequeueItemsInto(Ring.tailIndex(state), numDequeue)
				Some(newState)
			} else {
				None
			}
		}
	}
}
