package net.gfxmonk.capsul.mini2

import java.util.concurrent.ConcurrentLinkedQueue

import net.gfxmonk.capsul.UnitOfWork.{HasEnqueueAndResultPromise, HasEnqueuePromise}

import scala.annotation.tailrec
import net.gfxmonk.capsul.internal.Log
import net.gfxmonk.capsul.{AsyncTask, EnqueueableTask, StagedFuture, UnitOfWork}

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
private [capsul] object Ring {
	type Count = Int
	type Idx = Int

	// State is stored as a uint64. head & tail indices both get 4 bytes,
	// isRunning gets 1 and numQueued gets 7 bytes.
	val MAX_QUEUED = Math.pow(2, 8*2) // TODO: CHECK
	val MAX_SIZE = Math.pow(2, (8 * 4)-1) // TODO: CHECK
	def queueSpaceExhausted(t: State): Boolean = numQueued(t) == MAX_QUEUED

	// just for testing / debugging
	def repr(t:State) = {
		(headIndex(t), tailIndex(t), numQueued(t), isRunning(t))
	}


	// ------------------------------------------------
	// // # Simple implementation, for debugging
	// import monix.execution.atomic._
	// type State = (Int, Int, Int)
	// type AtomicState = AtomicAny[State]
	// def make(head: Idx, tail: Idx, numQueued: Count) = {
	// 	(head, tail, numQueued)
	// }
	// def headIndex(t:State):Idx = t._1
	// def tailIndex(t:State):Idx = t._2
	// def numQueued(t:State):Count = t._3
	// ------------------------------------------------
	// # Packed implementation, for performance. Head(2)|Tail(2)|NumQueued(4)
	import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
	def Atomic(n: Int): AtomicInteger = new AtomicInteger(n)
	def Atomic(n: Long): AtomicLong = new AtomicLong(n)
	type AtomicInt = AtomicInteger
	type State = Long
	type AtomicState = AtomicLong
	private val HEAD_OFFSET = 48 // 64 - 16
	private val TAIL_OFFSET = 32 // 64 - (2*16)
	private val IDX_MASK = 0xffff // 2 bytes (16 bits)
	private val RUNNING_MASK = 0x80000000L // bit 32 only
	private val QUEUED_MASK  = 0x7fffffffL // 4 bytes minus the top bit (31 bits)
	private def intOfRunning(running: Boolean): Long = if (running) RUNNING_MASK else 0x00

	def make(head: Idx, tail: Idx, numQueued: Count, running: Boolean) = {
		(head.toLong << HEAD_OFFSET) | (tail.toLong << TAIL_OFFSET) | (numQueued.toLong) | intOfRunning(running)
	}
	def headIndex(t:State):Idx = (t >>> HEAD_OFFSET).toInt
	def tailIndex(t:State):Idx = ((t >>> TAIL_OFFSET) & IDX_MASK).toInt
	def numQueued(t:State):Count = (t & QUEUED_MASK).toInt
	def isRunning(t:State):Boolean = (t & RUNNING_MASK) == RUNNING_MASK
	def incrementQueued(t:State):State = t+1L // queued is the lower bits, so simple addition works
	def setHead(t:State, h: Idx):State = make(h, tailIndex(t), numQueued(t), isRunning(t))
	def stopped(t:State): State = t & (~RUNNING_MASK)
	// ------------------------------------------------

}

private [capsul] class Ring[T >: Null <: AnyRef](size: Int) {
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

	def spaceAvailable(s: State):Int = {
		size - numItems(Ring.headIndex(s), Ring.tailIndex(s))
	}

	def dequeue(t:State, numDequeue: Int, numExtraSlots: Int): State = {
		// dequeue & mark as running
		val head = headIndex(t)
		val tail = tailIndex(t)
		make(head, add(tail, numDequeue + numExtraSlots), numQueued(t) - numDequeue, true)
	}

	def dequeueAndEnqueue(t:State, numDequeue: Int): State = {
		if (numDequeue == 0) {
			// just enqueue
			Ring.incrementQueued(t)
		} else {
			// dequeue `numDequeue` and enqueue one simultaneously, mark as running
			val head = headIndex(t)
			val tail = tailIndex(t)
			make(head, add(tail, numDequeue), (numQueued(t) - numDequeue) + 1, true)
		}
	}
}

object BackpressureExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new BackpressureExecutor(bufLen, Unordered)
	private val successfulUnit = Future.successful(())
}

sealed trait CompletionMode
case object Ordered extends CompletionMode
case object Unordered extends CompletionMode

class BackpressureExecutor(bufLen: Int, completionMode: CompletionMode)(implicit ec: ExecutionContext) {
	private [capsul] val ring = new Ring[AsyncTask](bufLen)
	private [capsul] val queue = new ConcurrentLinkedQueue[AsyncTask]()
	private [capsul] val stateRef:Ring.AtomicState = Ring.Atomic(Ring.make(0,0,0, false))

	import Log.log
	import Ring._

	// TODO decide on API
	def enqueue[R](fn: Function0[Future[R]]): Future[Unit] = {
		enqueueOnly(UnitOfWork.EnqueueOnlyAsync(fn))
	}
	def run[R](fn: Function0[Future[R]]): Future[Future[R]] = {
		enqueue(UnitOfWork.FullAsync(fn))
	}

	def enqueueOnly[R](task: AsyncTask with HasEnqueuePromise[Unit]): Future[Unit] = {
		if (doEnqueue(task)) {
			Future.unit
		} else {
			task.enqueuedPromise.future
		}
	}

	def enqueue[R](
		task: AsyncTask
			with UnitOfWork.HasEnqueuePromise[Future[R]]
			with UnitOfWork.HasResultPromise[R]
	): Future[Future[R]] = {
		if (doEnqueue(task)) {
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}

	private def startIfStopped(state: State) {
		if (!Ring.isRunning(state)) {
			ec.execute(workLoop)
		}
	}

	@tailrec
	private def doEnqueue(work: AsyncTask):Boolean = {
		val logId = Log.scope(this, "doEnqueue")
		val state = stateRef.get

		val numQueued = Ring.numQueued(state)
		val spaceAvailable = ring.spaceAvailable(state)
		val numDequeue = Math.min(spaceAvailable, numQueued)
		val hasSpaceForWork = spaceAvailable > numQueued

		// either we reserve `numQueued+1` slots, or we dequeue as many as we can and add one queued
		val nextState = if (hasSpaceForWork) {
			// reserve extra slot for this work
			ring.dequeue(state, numQueued, 1)
		} else {
			// dequeue possibly-zero waiting items and enqueue one for this work
			val next = ring.dequeueAndEnqueue(state, numDequeue)
			if (Ring.queueSpaceExhausted(next)) {
				// I can't imagine this happening, but an exception seems
				// better than corruption due to integer overflow.
				throw new RuntimeException(
					s"Overflow detected - ${Ring.MAX_QUEUED} items in a single Capsul queue"
				)
			}
			next
		}
		if (stateRef.compareAndSet(state, nextState)) {
			// We reserved all the slots we asked for, now assign into those slots
			// log(s"reserved ${numDequeue} dequeue and ${extraSlots} extra slots from ${Ring.tailIndex(state)}")
			val prevTail = Ring.tailIndex(state)
			val nextIdx:Idx = dequeueItemsInto(prevTail, numDequeue)
			if (hasSpaceForWork) {
				log(s"inserting work into $nextIdx")
				ring.at(nextIdx).set(work)
			} else {
				// we reserved a `queued` slot, so populate that
				queue.add(work)
			}
			startIfStopped(state)
			hasSpaceForWork
		} else {
			// couldn't reserve tail; retry
			doEnqueue(work)
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
		idx
	}

	// A runnable which repeatedly consumes & runs items in
	// the ring buffer until it's empty
	private val self = this
	val workLoop: Runnable = new Runnable() {
		@volatile private var nextTaskIndex: Int = 0
		private val completionFn: Function0[Unit] = completionMode match {
			case Ordered => completeFromHead
			case Unordered => completeUnordered
		}

		final def run(): Unit = {
			loop(1000, nextTaskIndex)
		}

		private def loop(maxItems: Int, headIndex: Int): Unit = {
			var state = stateRef.get
			val numRun = runBatch(maxItems, headIndex, state)
			maybeShutdown(maxItems - numRun, Ring.tailIndex(state))
		}

		private def runBatch(_maxItems: Int, headIndex: Int, state: Ring.State): Int = {
			val logId = Log.scope(self, "WorkLoop.runBatch")
			val currentTail = Ring.tailIndex(state)
			val readyItems = ring.numItems(headIndex, currentTail)

			// There must be something pending. Only this loop updates nextTaskIndex, and
			// it terminates if the ring becomes empty

			var currentHead = headIndex
			log(s"workLoop: ${Ring.repr(state)}, head = $currentHead, tail = $currentTail")
			while (currentHead != currentTail) {
				log(s"executing item @ $currentHead")
				val slot = ring.at(currentHead)
				var item = slot.get

				while(item == null) {
					// spin waiting for item to be set
					item = slot.get
				}

				runItem(item)
				if (completionMode == Unordered) {
					// TODO make nice
					slot.set(null)
				}
				currentHead = ring.inc(currentHead)
			}
			readyItems
		}

		@tailrec
		private def maybeShutdown(maxItems: Int, currentTail: Int): Unit = {
			// TODO: check for a changed head too, because that implies new space
			val logId = Log.scope(self, "WorkLoop.maybeShutdown")
			if (maxItems <= 0) {
				log("trampolining to prevent starvation")
				nextTaskIndex = currentTail
				ec.execute(workLoop)
			} else {
				val state = stateRef.get
				if (Ring.tailIndex(state) != currentTail) {
					// new tasks appeared in currentTail, re-run
					loop(maxItems, currentTail)
				} else {
					// needs to happen before we mark ourselves as stopped
					nextTaskIndex = currentTail
					if (stateRef.compareAndSet(state, Ring.stopped(state))) {
						// no new tasks, marked as stopped
						log(s"shutting down with state ${Ring.repr(state)}")
					} else {
						// CAS fail, retry loop
						maybeShutdown(maxItems, currentTail)
					}
				}
			}
		}

		private def runItem(item: AsyncTask): Unit = {
			val f = item.runAsync()
			if (f.isCompleted) {
				completionFn()
			} else {
				f.onComplete { _ =>
					completionFn()
				}
			}
		}

		private def completeFromHead(): Unit = {
			val state = stateRef.get
			val (didCompleteAny, finalState) = _completeFromHead(state, false)
			if (didCompleteAny) {
				tryDequeue(finalState)
			}
		}

		@tailrec
		private def completeUnordered(): Unit = {
			val state = stateRef.get
			val nextState = Ring.setHead(state, ring.inc(Ring.headIndex(state)))
			if (stateRef.compareAndSet(state, nextState)) {
				tryDequeue(nextState)
			} else {
				// CAS fail, try again
				completeUnordered()
			}
		}

		@tailrec
		private def tryDequeue(state: State): Unit = {
			// triggered when space has been made available, does nothing
			// if there's no pending work
			val logId = Log.scope(this, "tryDequeue")
			val numQueued = Ring.numQueued(state)
			if (numQueued > 0) {
				val spaceAvailable = ring.spaceAvailable(state)
				log(s"numQueued = $numQueued, spaceAvailable = $spaceAvailable")
				if (spaceAvailable > 0) {
					// try inserting at `tail`
					// log(s"space may be available (${Ring.repr(state)})")
					val numDequeue = Math.min(spaceAvailable, numQueued)
					// either we reserve `numQueued+1` slots, or we dequeue as many as we can and add one queued
					val nextState = ring.dequeue(state, numDequeue, 0)
					if (stateRef.compareAndSet(state, nextState)) {
						// We reserved all the slots we asked for, now assign into those slots
						// log(s"reserved ${numDequeue} dequeue and ${extraSlots} extra slots from ${Ring.tailIndex(state)}")
						val prevTail = Ring.tailIndex(state)
						val _:Idx = dequeueItemsInto(prevTail, numDequeue)
						startIfStopped(state)
					} else {
						// couldn't reserve tail; retry
						tryDequeue(stateRef.get)
					}
				} else {
					// no need to CAS, we get stateRef _after_ making space available so
					// if an enqueue runs after we get state it would see the free space
				}
			} else {
				// as above
			}
		}

		@tailrec
		private def _completeFromHead(state: Ring.State, didCompleteAny: Boolean): (Boolean, Ring.State) = {
			// "The fish completes from the head down..."
			// NOTE: may be called from any thread
			val headIndex = Ring.headIndex(state)
			val tailIndex = Ring.tailIndex(state)
			if (headIndex == tailIndex) {
				// empty ring
				(didCompleteAny, state)
			} else {
				val item = ring.at(headIndex).get
				val nextHead = ring.inc(headIndex)
				if (item != null && item.isComplete) {
					val nextState = Ring.make(nextHead, tailIndex, Ring.numQueued(state), Ring.isRunning(state))
					if (stateRef.compareAndSet(state, nextState)) {
						// done! try another
						_completeFromHead(nextState, true)
					} else {
						// CAS fail, try again
						_completeFromHead(stateRef.get, didCompleteAny)
					}
				} else {
					// incomplete item, stop there
					(didCompleteAny, state)
				}
			}
		}
	}
}
