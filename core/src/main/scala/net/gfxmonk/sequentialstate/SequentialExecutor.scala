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
 */

class RingItem[T<:AnyRef] {
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

	/// READY
	def readyIndex(t:Ready) = t._1
	def isRunning(t:Ready) = t._2
	def setRunning(t:Ready) = (t._1, true)
	def setStopped(t:Ready) = (t._1, false)
	def nextReadyValue(t: Tail) = (tailIndex(t), true)
}

class Ring[T <: AnyRef](size: Int) {
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
	private def inc(a: Idx) = add(a, 1)
	// actually adding a negative is problematic due to `%` behaviour for negatives
	def dec(a: Idx) = add(a, negativeOne)

	def spaceAvailable(h:Head, t:Tail):Boolean = {
		// queue is full when tail has wrapped around to one less than head
		inc(tailIndex(t)) != headIndex(h)
	}

	def nextTail(t:Tail) = (inc(tailIndex(t)), numQueued(t))
	def headActionableIndex(t:Head) = add(t._1, t._2)
}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

private final class Node[A](val item: A) {
	@volatile var next: Node[A] = null
	final def appearsAfter[A](parent: Node[A]): Boolean = {
		var node = parent
		while(node != null) {
			if (node == this) {
				return true
			}
			node = node.next
		}
		false
	}
}

private final class Queued[A](val len: Int, val node: Node[A]) {
	def add(newNode: Node[A]) = new Queued(len+1, newNode)
}
private object Queued {
	def single[A](node: Node[A]) = new Queued(1, node)
	def empty[A](node: Node[A]) = new Queued(0, node)
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

	private def setReady(insertedIdx: Idx, isWorkLoop: Boolean) {
		var currentReady = ring.readyRef.get
		var currentReadyIdx = Ring.readyIndex(currentReady)
		while(currentReadyIdx != ring.dec(insertedIdx)) {
			// some other thread needs to advance `ready` before we can, give them a chance
			// (TODO: should we spinlook a bit first?)
			LockSupport.parkNanos(100)
			currentReady = ring.readyRef.get
			currentReadyIdx = Ring.readyIndex(currentReady)
		}

		val nextReady = Ring.nextReadyValue(currentTail)
		if (isWorkLoop) {
			// blind write OK, nobody else will be stopping or incrementing idx
			ring.readyRef.set(nextReady)
		} else {
			while(true) {
				if (ring.readyRef.compareAndSet(currentReady, nextReady)) {
					// did we just set `running`? If so, it's our responsibility to spawn the run thread
					if (!isRunning(currentReady)) {
						spawnLoop()
					}
					break
				} else {
					// loop and try again, it has to succeed eventually
					currentReady = ring.readyRef.get
					nextReady = withRunningBit(setReadyIndex(currentReady, currentTail), true)
				}
			}
		}
	}

	private def incrementQueuedItems() {
		ring.tailRef.transform(Ring.incrementQueued)
	}

	private def decrementQueuedItems() {
		ring.tailRef.transform(Ring.decrementQueued)
	}

	private def insertWork(lastTail: Tail, currentTail: Tail, item: EnqueueableTask, isWorkLoop: Boolean) {
		// required: lastTail is reserved
		// returns: true if item was inserted, false if
		// item was interrupted by queued work
		var ret = true
		if (hasQueuedWork(lastTail)) {
			val queued = queue.pop()
			if (queued != null) {
				ring.at(lastTail).set(queued)
				setReady(Ring.tailIndex(lastTail), isWorkLoop)
				decrementQueuedItems()
				queued.notifyEnqueued()
				return false
			} // else someone already cleared out the queue
		}
		ring.at(lastTail).set(item)
		setReady(Ring.tailIndex(lastTail), isWorkLoop)
		return true
	}

	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val currentTail = ring.tailRef.get
		// first things first, is there spare capacity?
		val currentHead = head.get
		if (ring.spaceAvailable(currentHead, currentTail)) {
			// try inserting at `tail`
			val nextTail = ring.nextTail(currentTail)
			if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
				// OK, we have reserved the slot at `currentTail`.
				// We need to advance `ready` to our slot. This thread is the only one who can advance `ready`,
				// although others may be attempting to flip its other boolean bit(s)...
				if (insertWork(currentTail, nextTail, work)) {
					return true
				}
			}
			// couldn't reserve tail, or gazumped by queued work. retry
			return doEnqueue(work)
		} else {
			queue.push(work)
			incrementQueuedItems()
			return false
		}
	}

	trait WorkLoop extends Runnable {
		def numInProgress: Int
	}

	private val self = this // for logging

	// tail idx format:
	// Unsigned int, where 30 lowest bits are idx.
	// Highest two bits are:
	// 10: work enqueued (full)
	// 11: work enqueued, but space available
	// 00: nothing queued
	// 01: (never used)
	//
	// ready idx format:
	// *-- : whether the thread is currecntly scheduled
	//
	// Possibly in the future, it could consist of two 15-bit numbers which define:
	//  - number of pending futures
	//  - idx of ready item
	//
	//
	// HEAD (&& numFutures?)
	// ready (&& running)
	// tail (&& numQueued)

	val workLoop: WorkLoop = new WorkLoop() {
		final def run(): Unit = {
			val logId = Log.scope("WorkLoop.run")
			Log.log("begin")
			val headNode = head.get
			val inProgress = storedInProgress
			runNodeRec(headNode, 200, headNode, inProgress)
		}

		private def loop() {
			var currentHead = head.get
			while(true) {
				var currentReady = ring.readyRef.get
				var idx = currentHead
				while (idx < currentReady) {
					val item = ring.at(idx)
					item.run()
					idx++
					// TODO: if `chunk` exceeds half the capacity of the list, update head to relieve pressure
				}
				head.set(currentReady)
				currentHead = currentReady

				while(true) {
					currentReady = ring.readyRef.get
					if (Ring.readyIndex(currentReady) == Ring.readyIndex(currentHead)) {
						// no more work. Is there queued work though?
						val currentTail = ring.tailRef.get
						if (hasQueuedWork(currentTail) && ring.spaceAvailable(currentHead, currentTail)) {
							// try reserving a tail slot
							val nextTail = currentTail + 1
							if (ring.tailRef.compareAndSet(currentTail, nextTail)) {
								insertWork(currentTail, nextTail, Work.noop)
								break
							} else {
								// couldn't reserve, retry
								continue
							}
						} else {
							// no queued work, or no space avilable
							if (ring.readyRef.compareAndSet(currentReady, setStopped(currentReady))) {
								return
							} // else `ready` changed, loop again
						}
					} else {
						// ready is less than head, we've already got work to do
						break
					}
				}
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

