package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import net.gfxmonk.sequentialstate.internal.Log

import monix.execution.atomic.Atomic

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
 */

class RingItem[T>:Null<:AnyRef] {
	@volatile private var contents:T = null
	def set(item:T) { contents = item }
	def get: T = contents
}

object Ring {
	type Idx = Int
	type QueueSize = Int
	type PromiseCount = Int
	type Tail = (Idx, QueueSize)
	type Head = (Idx, PromiseCount)
	type Ready = (Idx, Boolean)

	// TAIL
	def tailIndex(t:Tail) = t._1
	def numQueued(t:Tail) = t._2
	def hasQueuedWork(t:Tail) = numQueued(t) != 0
	def incrementQueued(t:Tail) = (tailIndex(t), numQueued(t) + 1)
	def decrementQueued(t:Tail) = (tailIndex(t), numQueued(t) - 1)

	// HEAD
	def headIndex(t:Head) = t._1
	def numRunningFutures(t:Head) = t._2
	def head(i:Idx, n: Int) = (i, n)
	def incrementRunningFutures(t:Head) = (headIndex(t), numRunningFutures(t) + 1)
	def withHeadIndex(t:Head, i: Idx) = (i, numRunningFutures(t))

	/// READY
	def readyIndex(t:Ready) = t._1
	def isRunning(t:Ready) = t._2
	def setRunning(t:Ready) = (t._1, true)
	def setStopped(t:Ready) = (t._1, false)
	def withIndex(i: Idx) = (i, true)
}

class Ring[T >: Null <: AnyRef](size: Int) {
	import Ring._
	val queue = new ConcurrentLinkedQueue[T]()
	val headRef = Atomic((0,0))
	val tailRef = Atomic((0,0))
	val readyRef = Atomic((0,false))

	private val bounds = size + 1
	private val negativeOne = size
	private var arr = Array.fill(bounds)(new RingItem[T])

	// RING
	def at(idx: Idx) = arr(idx) // unsafe woo
	def add(a: Idx, b: Int) = {
		(a + b) % bounds
	}
	def inc(a: Idx) = add(a, 1)
	// actually adding a negative is problematic due to `%` behaviour for negatives
	def dec(a: Idx) = add(a, negativeOne)

	def spaceAvailable(h:Head, t:Tail):Boolean = {
		// queue is full when tail has wrapped around to one less than head
		inc(tailIndex(t)) != headIndex(h)
	}

	def nextTail(t:Tail) = (inc(tailIndex(t)), numQueued(t))
	def headActionableIndex(t:Head) = add(t._1, t._2)
	def decrementRunningFutures(t:Head) = (inc(headIndex(t)), numRunningFutures(t) - 1)
	def addToHeadIndex(t:Head, n: Int) = (add(headIndex(t), n), numRunningFutures(t))
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

	private def setReady(currentTail: Tail, isWorkLoop: Boolean) {
		val logId = Log.scope("Executor.setReady")
		var currentReady = ring.readyRef.get
		log(s"ready = $currentReady")
		var currentReadyIdx = Ring.readyIndex(currentReady)
		val index = Ring.tailIndex(currentTail)
		val previousIndex = ring.dec(index)
		while(currentReadyIdx != previousIndex) {
			// some other thread needs to advance `ready` before we can, give them a chance
			// (TODO: should we spinlook a bit first?)
			log(s"readyIdx = $currentReadyIdx, awaiting $previousIndex")
			LockSupport.parkNanos(100)
			currentReady = ring.readyRef.get
			currentReadyIdx = Ring.readyIndex(currentReady)
		}

		var nextReady = Ring.withIndex(index)
		if (isWorkLoop) {
			// blind write OK, nobody else will be stopping or incrementing idx
			log(s"readyRef.set($nextReady)")
			ring.readyRef.set(nextReady)
		} else {
			while(!ring.readyRef.compareAndSet(currentReady, nextReady)) {
				// loop and try again, it has to succeed eventually
				currentReady = ring.readyRef.get
			}
			log(s"readyRef.compareAndSet($currentReady, $nextReady)")
			// did we just set `running`? If so, it's our responsibility to spawn the run thread
			if (!isRunning(currentReady)) {
				log("spawning workLoop")
				ec.execute(workLoop)
			}
		}
	}

	private def insertWork(lastTail: Tail, currentTail: Tail, item: EnqueueableTask, isWorkLoop: Boolean): Boolean = {
		// required: lastTail is reserved
		// returns: true if item was inserted, false if
		// item was interrupted by queued work
		val logId = Log.scope("Executor.insertWork")
		val tailIndex = Ring.tailIndex(lastTail)
		if (hasQueuedWork(lastTail)) {
			val queued = ring.queue.poll()
			log(s"dequeued item $queued")
			if (queued != null) {
				ring.at(tailIndex).set(queued)
				setReady(currentTail, isWorkLoop)
				ring.tailRef.transform(Ring.decrementQueued)
				log("decremented quqeued items")
				queued.enqueuedAsync()
				return false
			} // else someone already cleared out the queue
		}
		log(s"no queued items, inserting work")
		ring.at(tailIndex).set(item)
		setReady(currentTail, isWorkLoop)
		return true
	}

	@tailrec
	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val logId = Log.scope("Executor.doEnqueue")
		val currentTail = ring.tailRef.get
		// first things first, is there spare capacity?
		val currentHead = ring.headRef.get
		if (ring.spaceAvailable(currentHead, currentTail)) {
			// try inserting at `tail`
			log(s"space may be available ($currentHead, $currentTail)")
			val nextTail = ring.nextTail(currentTail)
			if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
				// OK, we have reserved the slot at `currentTail`.
				// We need to advance `ready` to our slot. This thread is the only one who can advance `ready`,
				// although others may be attempting to flip its other boolean bit(s)...
				log(s"reserved slot $currentTail")
				if (insertWork(currentTail, nextTail, work, false)) {
					return true
				} else {
					log(s"slot taken by queued item; retrying")
				}
			}
			// couldn't reserve tail, or gazumped by queued work. retry
			return doEnqueue(work)
		} else {
			log(s"putting item in queue ($currentHead, $currentTail)")
			ring.queue.add(work)
			ring.tailRef.transform(Ring.incrementQueued)
			// while(true) {
			// 	// TODO need to ensure workLoop is running.
			// 	val ready = ring.readyRef.get
			// 	if (!Ring.ready
			// 	if (ring.readyRef.compareAndSet(ready, 
			// }
			return false
		}
	}

	val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			Log("begin")
			loop(200)
		}

		@tailrec
		private def loop(_maxItems: Int): Unit = {
			val logId = Log.scope("WorkLoop.run")
			var currentHead = ring.headRef.get
			var currentReady = ring.readyRef.get
			var index = ring.headActionableIndex(currentHead)
			var readyIndex = Ring.readyIndex(currentReady)

			var maxItems = _maxItems
			var unacknowledgedCompletions = 0
			log(s"workLoop: index = $index, readyIndex = $readyIndex")
			while (index != readyIndex) {
				log(s"executing item @ $index")
				val item = ring.at(index)
				index = ring.inc(index)
				item.get.run().filter(!_.isCompleted) match {
					case None => {
						log("ran sync node")
						// we don't have to acknowledge every sync item, we can do it in a batch to save CAS operations.
						// But don't let HEAD lag behind reality more than `bufLen / 2`
						unacknowledgedCompletions += 1
						if (unacknowledgedCompletions >= bufLen / 2) {
							if(ring.headRef.compareAndSet(currentHead, Ring.withHeadIndex(currentHead, index))) {
								currentHead = ring.headRef.get
							} else {
								unacknowledgedCompletions = 0
							}
						}
					}
					case Some(f) => {
						log("ran async node")
						// increment head's async counter
						var nextHead = ring.addToHeadIndex(Ring.incrementRunningFutures(currentHead), unacknowledgedCompletions)
						while (!ring.headRef.compareAndSet(currentHead, nextHead)) {
							currentHead = ring.headRef.get
							nextHead = ring.addToHeadIndex(Ring.incrementRunningFutures(currentHead), unacknowledgedCompletions)
						}
						unacknowledgedCompletions = 0
						// now that we've incremented running futures, set it up to decrement on completion
						f.onComplete { _ =>
							ring.headRef.transform(ring.decrementRunningFutures)
							//TODO: ensure workLoop is running
						}
					}
				}
				maxItems = maxItems - 1
			}

			if (unacknowledgedCompletions > 0) {
				log(s"incrementing HEAD idx by $unacknowledgedCompletions")
				ring.headRef.transform(head => ring.addToHeadIndex(head, unacknowledgedCompletions))
			}

			if (shouldKeepRunningAfterBatch(currentHead)) {
				if (maxItems <= 0) {
					log("trampoline")
					return ec.execute(workLoop)
				} else {
					return loop(maxItems)
				}
			}
		}

		@tailrec
		private def shouldKeepRunningAfterBatch(currentHead: Head): Boolean = {
			val logId = Log.scope("WorkLoop.shouldKeepRunningAfterBatch")
			val currentReady = ring.readyRef.get
			if (Ring.readyIndex(currentReady) == ring.headActionableIndex(currentHead)) {
				// no more work. Is there queued work though?
				val currentTail = ring.tailRef.get
				if (hasQueuedWork(currentTail) && ring.spaceAvailable(currentHead, currentTail)) {
					// try reserving a tail slot
					val nextTail = ring.nextTail(currentTail)
					if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
						log(s"attempting to dequeue work into slot $currentTail")
						insertWork(currentTail, nextTail, UnitOfWork.noop, true)
						return true // either we dequeued work, or we were beaten to it
						// TODO is it possible for this to spin, if the thread which was decrementing
						// the tail queue count is interrupted?
					} else {
						// couldn't reserve slot, retry
						return shouldKeepRunningAfterBatch(currentHead)
					}
				} else {
					// no queued work, or no space avilable
					log(s"no queued work, or no space available")
					if (ring.readyRef.compareAndSet(currentReady, setStopped(currentReady))) {
						log("stopping")
						return false
					} else {
						// `ready` changed, loop again
						log("stop request failed, trying again")
						return shouldKeepRunningAfterBatch(currentHead)
					}
				}
			} else {
				// ready is less than head, we've already got work to do
				log("more work detected")
				return true
			}
		}
	}
}

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

