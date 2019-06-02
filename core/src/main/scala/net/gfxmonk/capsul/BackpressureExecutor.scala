package net.gfxmonk.capsul

import java.util.concurrent.ConcurrentLinkedQueue

import net.gfxmonk.capsul
import net.gfxmonk.capsul.StagedWork.HasEnqueuePromise
import net.gfxmonk.capsul.internal.Log

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

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

object BackpressureExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new BackpressureExecutor(bufLen)
	private val successfulUnit = Future.successful(())

	private [capsul] object Ring {
		import capsul.BaseRing._

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
		import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
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

	private [capsul] class Ring[T >: Null <: AnyRef](size: Int) extends capsul.BaseRing[T](size) {
		import capsul.BaseRing._
		import Ring._
		if (size > MAX_SIZE) {
			throw new RuntimeException(s"size ($size) is larger then the maximum ($MAX_SIZE)")
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
}

class BackpressureExecutor(bufLen: Int)(implicit ec: ExecutionContext) extends CapsulExecutor {
	import BackpressureExecutor._
	import BackpressureExecutor.Ring._
	import Log.log
	import capsul.BaseRing._

	private [capsul] val ring = new Ring[StagedWork](bufLen)
	private [capsul] val queue = new ConcurrentLinkedQueue[StagedWork]()
	private [capsul] val stateRef:Ring.AtomicState = Ring.Atomic(Ring.make(0,0,0, false))

	// TODO decide on API
	def sendAsync[R](fn: Function0[Future[R]]): Future[Unit] = {
		enqueueOnly(StagedWork.EnqueueOnlyAsync(fn))
	}
	def send[R](fn: Function0[_]): Future[Unit] = {
		enqueueOnly(StagedWork.EnqueueOnly(fn))
	}
	def run[R](fn: Function0[R]): Future[Future[R]] = {
		enqueue(StagedWork.Full(fn))
	}
	def runAsync[R](fn: Function0[Future[R]]): Future[Future[R]] = {
		enqueue(StagedWork.FullAsync(fn))
	}
	def flatRunAsync[R](fn: Function0[Future[R]]): Future[R] = {
		enqueue(StagedWork.FullAsync(fn)).flatten
	}

	final override def enqueueOnly[R](task: StagedWork with HasEnqueuePromise[Unit]): Future[Unit] = {
		if (doEnqueue(task)) {
			Future.unit
		} else {
			task.enqueuedPromise.future
		}
	}

	final override def enqueue[R](
		task: StagedWork
			with StagedWork.HasEnqueuePromise[Future[R]]
			with HasResultPromise[R]
	): Future[Future[R]] = {
		if (doEnqueue(task)) {
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}

	private def startIfStopped(prevState: State) {
		if (!Ring.isRunning(prevState)) {
			ec.execute(workLoop)
		}
	}

	private def startIfNecessary(prevState: State, nextState: State) {
		if (Ring.isRunning(nextState)) startIfStopped(prevState)
	}

	@tailrec
	private def doEnqueue(work: StagedWork):Boolean = {
		val logId = Log.scope(this, "doEnqueue")
		val state = stateRef.get

		val numQueued = Ring.numQueued(state)
		val spaceAvailable = ring.spaceAvailable(state)
		val hasSpaceForWork = spaceAvailable > numQueued
		log(s"spaceAvailable = ${spaceAvailable}, numQueued = ${numQueued} from state ${Ring.repr(state)}")
		val numDequeue = if (hasSpaceForWork) numQueued else spaceAvailable

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
				log(s"setting item @ slot idx $nextIdx after dequeueing $numDequeue (${Ring.repr(nextState)})}")
				ring.at(nextIdx).set(work)
			} else {
			// final slot is current tail + numQueued
				log(s"dequeued $numDequeue pending items; queueing work due to full buffer (${Ring.repr(nextState)}). Final destination @ slot idx ${ring.add(nextIdx, Ring.numQueued(nextState))}")
				// we reserved a `queued` slot, so populate that
				queue.add(work)
			}
			startIfNecessary(state, nextState)
			hasSpaceForWork
		} else {
			// couldn't reserve tail; retry
			doEnqueue(work)
		}
	}

	private def dequeueItemsInto(dest: Idx, numItems: Int): Idx = {
		val logId = Log.scope(this, s"dequeueItemsInto($dest, $numItems)")
		var idx = dest
		var nextTail = ring.add(dest, numItems)
		while(idx != nextTail) {
			var queued = queue.poll()
			while (queued == null) {
				// spinloop, since reserved (but un-populated) slots
				// in the ring will hold up the executor (and we
				// also run this from the executor)
				log(s"awaiting item @ slot idx $idx")
				queued = queue.poll()
			}
			log(s"setting dequeued item @ slot idx $idx")
			ring.at(idx).set(queued)
			queued.enqueuedAsync()
			idx = ring.inc(idx)
		}
		nextTail
	}

	// A runnable which repeatedly consumes & runs items in
	// the ring buffer until it's empty
	private val self = this
	val workLoop: Runnable = new Runnable() {
		@volatile private var nextTaskIndex: Int = 0
		private val completionFn: Function1[Any,Unit] = _ => completeUnordered(1)

		final def run(): Unit = {
			loop(1000, nextTaskIndex)
		}

		private def loop(maxItems: Int, headIndex: Int): Unit = {
			var state = stateRef.get
			val logId = Log.scope(self, "WorkLoop.runBatch")
			val currentTail = Ring.tailIndex(state)
			var currentHead = headIndex
			val readyItems = ring.numItems(headIndex, currentTail)
			var completedSync = 0

			// There must be something pending. Only this thread updates nextTaskIndex
			// (in maybeShutdown), and it terminates if the ring becomes empty

			log(s"workLoop: ${Ring.repr(state)}, head = $currentHead, tail = $currentTail")
			while (currentHead != currentTail) {
				log(s"loading item @ slot idx $currentHead")
				val slot = ring.at(currentHead)
				var item = slot.get

				while (item == null) {
					// spin waiting for item to be set
//					val now = System.currentTimeMillis // NOCOMMIT
//					if (now  - startTime > 1000) {
//						log(s"Still waiting for item @ slot idx $currentHead")
//						startTime = now
//					}
					item = slot.get
				}
				log(s"nulling item @ slot idx $currentHead")
				slot.set(null)

				log(s"running item @ slot idx $currentHead")
				item.spawn() match {
					case Completed => {
						// already done, can complete sync
						completedSync += 1
					}
					case Async(f) => f.onComplete(completionFn)
				}

				currentHead = ring.inc(currentHead)
			}
			if (completedSync > 0) {
				completeUnordered(completedSync)
			}
			maybeShutdown(maxItems - readyItems, Ring.tailIndex(state))
		}

		@tailrec
		private def maybeShutdown(maxItems: Int, currentTail: Int): Unit = {
			// Note: headIndex can change from other threads, but it never
			// exceeds nextTaskIndex so we don't need to consider it here
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
					val stopped = Ring.stopped(state)
					if (stateRef.compareAndSet(state, stopped)) {
						// no new tasks, marked as stopped
						log(s"shutting down with state ${Ring.repr(stopped)}")
					} else {
						// CAS fail, retry loop
						maybeShutdown(maxItems, currentTail)
					}
				}
			}
		}

		@tailrec
		private def completeUnordered(n: Int): Unit = {
			val logId = Log.scope(self, s"WorkLoop.completeUnordered($n)")
			val state = stateRef.get
			val nextState = Ring.setHead(state, ring.add(Ring.headIndex(state), n))
			if (stateRef.compareAndSet(state, nextState)) {
				log(s"updated state to ${Ring.repr(nextState)}, attempting dequeue")
				tryDequeue(nextState)
			} else {
				// CAS fail, try again
				completeUnordered(n)
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
						log(s"reserved $numDequeue slots, populating them (${Ring.repr(nextState)})")
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
	}
}
