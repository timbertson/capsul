package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import scala.annotation.tailrec
import net.gfxmonk.sequentialstate.internal.Log

import monix.execution.atomic.Atomic

import scala.concurrent.{ExecutionContext, Future}

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
	private val nullNode: Node[EnqueueableTask] = null
	private val head = Atomic(nullNode)
	private val queued = Atomic(Queued.empty(nullNode))
	private val tail = Atomic(nullNode)
	import Log.log

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

	private def didEnqueueSingleItem() {
		// either we inserted an item but couldn't mark the `tail` as having queued work,
		// or we inserted an item somewhere at the end of the queue.
		val currentTail = tail.get
		if (hasQueuedWork(currentTail)) {
			// OK, we know there's queued work already
		} else {
			// space just cleared up. Either the ring is _precisely_ full, or there's now room.
			val currentHead = head.get
			if (spaceAvailable(currentHead, currentTail) {
				// try reserving a slot
				val nextTail = currentTail + 1
				if (tail.compareAndSet(currentTail, nextTail)) {
					// OK, we have reserved the slot at `currentTail`.
					// pop the work we just added on tail instead
					item = queue.pop()
					if (item == null) {
						// Someone already dequeued our item. We can't give back our slot, but we can set it to a
						// noop
						return
					} else {
						ring(currentTail).set(item)
						setReady(currentTail)
					}
				} else {
					// couldn't get that tail slot, just loop and try again
					didEnqueueSingleItem()
				}
			} else {
				// no space available, try setting the queued flag
				if (tail.compareAndSet(currentTail, markEnqueued(currentTail))) {
					return
				} else {
					didEnqueueSingleItem()
				}
			}
		}
	}

	private def setReady(tailIdx) {
		var currentReady = ready.get
		while(currentReadyIdx != ringAdd(currentTail, -1)) {
			var currentReadyIdx = lowerBits(currentReady)
			// some other thread needs to advance `ready` before we can, give them a chance
			// (TODO: should we spinlook a bit first?)
			Thread.nanosleep();
			currentReady = ready.get
		}

		while(true) {
			val currentReadyIdx = lowerBits(currentReady)
			val nextReady = withRunningBit(setReadyIndex(currentReady, currentTail), true)
			if (ready.compareAndSet(currentReady, setReadyIndex(currentReady, currentTail))) {
				// did we just set `running`? If so, it's our responsibility to spawn the run thread
				if (!isRunning(currentReady)) {
					spawnLoop()
				}
				break
			} else {
				currentReady = ready.get
				// loop and try again
			}
		}
	}

	private def doEnqueue(work: EnqueueableTask):Boolean = {
		val currentTail = tail.get
		// first things first, is there spare capacity?
		if (hasQueuedWork(currentTail)) {
			queue.push(work)
			didEnqueueSingleItem()
		} else {
			// queue may have room:
			val currentHead = head.get
			if (spaceAvailable(currentHead, currentTail)) {
				// try inserting at `tail`
				val nextTail = currentTail + 1
				if (tail.compareAndSet(currentTail, nextTail)) {
					// OK, we have reserved the slot at `currentTail`.
					// We need to advance `ready` to our slot. This thread is the only one who can advance `ready`,
					// although others may be attempting to flip its other boolean bit(s)...
					ring(currentTail).set(work)

					setReady(currentTail)
					return Accepted;
				}
			} else {
				// Start the queue
				enqueueQueue.push(work);
				if (tail.compareAndSet(currentTail, markEnqueued(currentTail))) {
					// ok, we've marked it as enqueued
					return
				} else {
					didEnqueueSingleItem()
				}
			}
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
	// Or, perhaps the HEAD pointer saves the number of pending promises? We'd update it with
	// the 

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
			var currentReady = ready.get
			while(true) {
				var idx = currentHead
				for (idx = currentHead; idx <= currentReady; idx++) {
					val item = ring(idx)
					item.run()
					// TODO: if we're struggling for space, attempt to advance HEAD so that a producer has a chance to continue
				}
				head.set(currentReady)

				var signalledSpaceReady = false
				while(true) {
					val tailVal = tail.get
					if (hasQueuedWork(tailVal) && !hasSpaceAvailable(tailVal)) {
						// try to update tail
						if (tail.compareAndSet(tailVal, markSpaceAvailable(tailVal))) {
							// ok, signalled
							signalledSpaceReady = true
							break
						}
					} else {
						break // tail doesn't need update
					}
				}

				if (ready.compareAndSet(currentReady, withStoppedBit(currentReady))) {
					// there is no ready work, and we've marked the thread as stopped.
					// One thing we could do is enqueue more work though
					if (signalledSpaceReady) {
						// TODO try and dequeue some stuff.
					}
					return park()
				} else {
					// TODO: if we've run over `n` items, yield the thread instead
					val nextReady = ready.get
					assert (lowerBits(nextReady) != lowerBits(currentReady)) // ready should have only been changed to advance it
					currentHead = currentReady
					currentReady = nextReady
					continue
				}
			}
		}
	}
}
