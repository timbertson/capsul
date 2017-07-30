package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}

import org.jctools.queues.{MpscLinkedQueue, MessagePassingQueue}
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

private final class Node[A](val item: A) {
	@volatile var next: Node[A] = null
	final def appearsAfter[A](parent: Node[A]): Boolean = {
		var node = parent.next
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

	private def enqueue(work: EnqueueableTask):Boolean = {
		var current = tail.get
		val updated = new Node(work)

		// may race with other enqueuer threads
		// var printlnTailCount = 1
		while(!tail.compareAndSet(current, updated)) {
			// printlnTailCount += 1
			current = tail.get
		}

		if (current == null) {
			// we're the first item! runloop is definitely not running
			queued.set(Queued.single(updated))
			head.set(updated)
			ec.execute(workLoop)
			true
		} else {
			// nodes must be present in the linked list before adding them to `queued`
			// (also we want the consumer to see new tasks ASAP)
			current.next = updated

			// now update `queued`
			// var printlnQueueCount = 0
			while(true) {
				// printlnQueueCount += 1
				val currentQueued = queued.get
				if (currentQueued.len == bufLen) {
					// we're at capacity - only the consumer can advance queued
					// (and it already knows about our new node because we've put it in tail)
					return false
				} else {
					if (currentQueued.node.eq(current)) {
						// common case: we added a node, and now we can enqueue it
						if (queued.compareAndSet(currentQueued, currentQueued.add(updated))) {
							// println("queued is now " + (currentQueued.len+1))
							// if(printlnTailCount > 1 || printlnQueueCount > 1) println("tail attempts: " + printlnTailCount + ", queued attempts = " + printlnQueueCount)
							return true
						}
					} else if (currentQueued.node.eq(updated)) {
						// runner thread has already enqueued us
						// println("enqueue(): someone else enqueued...")
						return true
					} else {
						// Either the currentQueued is before us (lagging behind `tail`),
						if (updated.appearsAfter(currentQueued.node)) {
							// This means another thread must update `queued` before we can do our bit.
							// Sleep a little then try again.
							// println("enqueue(): had a race; sleep")
							LockSupport.parkNanos(2000)
						} else {
							// OR, there are two possibilities:
							//  - the queue has advanced past us
							//  - the entire stack that we were added to has been completed,
							//    and queued is now a new stack
							// In either case we've definitely been queued
							// println("enqueue(): queued advanced past us; skipping")
							return true
						}
					}
				}
			}

			assert(false); false // unreachable
		}
	}

	val workLoop:Runnable = new Runnable() {
		def run() {
			// println("start")
			var maxIterations = 200

			var headNode = head.get

			var node = headNode
			while(maxIterations > 0) {
				node.item.run()
				maxIterations -= 1

				while (node.next == null) {
					// looks like we've hit the tail.
					if (tail.compareAndSet(node, null)) {
						// yeah, that was the last item.

						// Note: there's no need to reset either head or queued, as once tail is null
						// the enqueuer will reset both of these before kicking off a new work loop.
						// We do attempt to null out `head` because it could prevent GC of
						// up to 200 tasks otherwise
						head.compareAndSet(headNode, null)
						// println("done " + (200 - maxIterations) + " tasks")
						return
					} else {
						// println("spin() node.next")
						// `node` isn't really the tail, so we just need to spin, waiting for
						// its `.next` property to be set
					}
				}
				node = node.next

				// update queued
				var updated = false
				while (!updated) {
					val currentQueued = queued.get
					if (currentQueued.len == bufLen) {
						// we're at capacity, only this thread can advance it (just use `set`)
						val queuedNode = currentQueued.node
						val nextQueued = queuedNode.next
						if (nextQueued == null) {
							queued.set(new Queued(bufLen-1, queuedNode))
						} else {
							queued.set(new Queued(bufLen, nextQueued))
							// notify the lucky winner
							nextQueued.item.enqueuedAsync()
						}
						updated = true
					} else {
						// We're not at capacity. If threads enqueue more tasks in the meantime
						// this CAS will fail and we'll loop again
						// println("queued = " + currentQueued)
						updated = queued.compareAndSet(currentQueued, currentQueued)
					}
				}
			}

			// we ran 200 iterations.
			// update `head` and reenqueue this task so we don't hog this thread
			if (!head.compareAndSet(headNode, node)) {
				throw new IllegalStateException("head modified by external thread")
			}
			ec.execute(workLoop)
		}
	}

	def enqueueOnly[R](fun: Function0[R]): Future[Unit] = {
//		enqueueAsync(fun).map((_:Future[R]) => ())

		val task = UnitOfWork.EnqueueOnly(fun, bufLen)
		if (enqueue(task)) {
			SequentialExecutor.successfulUnit
		} else {
			task.enqueuedPromise.future
		}
	}

	def enqueueReturn[R](fun: Function0[R]): Future[R] = {
//		enqueueAsync(fun).flatMap(identity)

		val task = UnitOfWork.ReturnOnly(fun, bufLen)
		enqueue(task)
		task.resultPromise.future
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork.Full(fun, bufLen)
		if (enqueue(task)) {
//			println("immediate!")
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}
}
