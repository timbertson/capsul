package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport
import scala.annotation.tailrec

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

	private def doEnqueue(work: EnqueueableTask):Boolean = {
		var currentTail = tail.get
		val newTail = new Node(work)
		// val log = Log.id(s"doEnqueue(${System.identityHashCode(newTail)})")

		// may race with other enqueuer threads
		// var logTailCount = 1
		while(!tail.compareAndSet(currentTail, newTail)) {
			// logTailCount += 1
			// if(logTailCount > 5) log("logTailCount: " + logTailCount)
			currentTail = tail.get
		}
		// log("added to tail")

		if (currentTail == null) {
			// log(s"currentTail == null; beginning loop with ${workLoop.numInProgress} in progress items")
			// we're the first item! runloop is definitely not running
			queued.set(new Queued(workLoop.numInProgress + 1, newTail))
			head.set(newTail)
			ec.execute(workLoop)
			true
		} else {
			// nodes must be present in the linked list before adding them to `queued`
			// (also we want the consumer to see new tasks ASAP)
			currentTail.next = newTail
			// log(s"set currentTail.next")

			// now update `queued`
			// var logQueueCount = 0
			while(true) {
				// logQueueCount += 1
				// if(logQueueCount > 5) log("..ongoing queue attempts: " + logQueueCount)
				val currentQueued = queued.get
				if (currentQueued.len == bufLen) {
					// log("at capacity; not advancing queued")
					return false
				} else {
					if (currentQueued.node.eq(currentTail)) {
						// common case: we added a node, and now we can (try to) enqueue it
						if (queued.compareAndSet(currentQueued, currentQueued.add(newTail))) {
							// log(s"below capacity (${currentQueued.len} < ${bufLen}, advanced node by one")
							return true
						}
					} else if (currentQueued.node.eq(newTail)) {
						// log("below capacity, but new node is already enqueued")
						return true
					} else {
						val queuedNode = currentQueued.node
						if (newTail.appearsAfter(queuedNode)) {
							// We need `queued` to advance until either we reach buflen or the
							// task we added, and any thread knows enough to do that. Give it a go,
							// but don't retry if someone beats us to it.
							// log("below capacity; attempting to advance queued by one")
							val _:Boolean = queued.compareAndSet(currentQueued, currentQueued.add(currentQueued.node.next))
						} else if (queuedNode != null && queuedNode.appearsAfter(newTail)) {
								// log("another thread has advanced `queued` past the node I inserted");
								return true;
						} else {
							// current chain may be new, and the enqueuer has not initialized queued yet
							// log("node and queued are not in the same chain; spinning until consistent");
							LockSupport.parkNanos(100)
						}
					}
				}
			}

			assert(false); false // unreachable
		}
	}

	trait WorkLoop extends Runnable {
		def numInProgress: Int
	}

	val workLoop: WorkLoop = new WorkLoop() {
		@volatile private var storedInProgress: List[Future[_]] = Nil

		final def numInProgress = storedInProgress.length

		final def run(): Unit = {
			// Log.id("WorkLoop.run")("begin")
			val headNode = head.get
			val inProgress = storedInProgress
			runNodeRec(headNode, 200, headNode, inProgress)
		}

		private def suspendWithInProgress(
			headNode: Node[EnqueueableTask],
			completedNode: Node[EnqueueableTask],
			inProgress: List[Future[_]]
		): Unit = {
			// val log = Log.id("suspendWithInProgress")
			// log(s"suspending with ${inProgress.length}")
			Future.firstCompletedOf[Any](inProgress).onComplete { _ =>
				inProgress.partition(_.isCompleted) match {
					case (Nil, inProgress) => assert(false)
					case (completed, inProgress) => {
						// log(s"${completed.length} of ${inProgress.length} completed!")
						advanceQueued(completed.length)
						val headNode = head.get
						val nextNode = advanceNode(headNode, completedNode, inProgress)
						if (nextNode != null) {
							runNodeRec(headNode, 200, nextNode, inProgress)
						}
					}
				}
			}
		}

		@tailrec private final def runNodeRec(
			headNode: Node[EnqueueableTask],
			numIterations: Int,
			node: Node[EnqueueableTask],
			inProgress: List[Future[_]]
		) {
			// val log = Log.id("runNodeRec")
			if (numIterations > 0) {
				val asyncCompletion = node.item.run()
				asyncCompletion match {
					case None => {
						// log("ran sync node")
						completeNodes(1, node, inProgress) match {
							case None => assert(false) // we can always advance 1 node
							case Some(inProgress) => {
								val next = advanceNode(headNode, node, inProgress)
								if (next != null) {
									runNodeRec(headNode, numIterations - 1, next, inProgress)
								}
							}
						}
					}
					case Some(f) => {
						// log("ran async node")
						completeNodes(0, node, f :: inProgress) match {
							case None => {
								// log("nothing to dequeue; suspending")
								suspendWithInProgress(headNode, node, inProgress)
							}
							case Some(inProgress) => {
								// log("there are tasks ready to execute")
								val next = advanceNode(headNode, node, inProgress)
								if (next != null) {
									runNodeRec(headNode, numIterations - 1, next, inProgress)
								}
							}
						}
					}
				}
			} else {
				// log("ran 200 iterations; trampolining")
				storedInProgress = inProgress
				if (!head.compareAndSet(headNode, node)) {
					throw new IllegalStateException("head modified by external thread")
				}
				ec.execute(workLoop)
			}
		}

		// returns
		// None => cannot continue; at capacity
		// Some(list) => new inProgress list (with complete items removed)
		private final def completeNodes(
			completedSync: Int,
			completed: Node[EnqueueableTask],
			inProgress: List[Future[_]]): Option[List[Future[_]]] =
		{
			// val log = Log.id(if (completedSync == 0) "completeNodesAsync" else "completeNotesSync")
			@tailrec def tryUpdate():Option[List[Future[_]]] = {
				val currentQueued = queued.get
				inProgress.partition(_.isCompleted) match {
					// Are some of these items already done?
					case (Nil, inProgress) => {
						// Nothing async has completed yet
						if (completedSync != 0) {
							// log(s"advancing $completedSync sync item")
							advanceQueued(completedSync)
							Some(inProgress)
						} else {
							// log("nothing async completed")
							if (currentQueued.len == bufLen) {
								// log("we're at capacity (so nobody else will update queued)")
								// log("did we just run the final queued item?")
								// We're at capacity. Did we just complete the final queued item?
								if (currentQueued.node.eq(completed)) {
									// log("yes; returning None")
									None
								} else {
									// log("no, we have queued items. Returning unchanged inProgress")
									Some(inProgress)
								}
							} else {
								if (queued.compareAndSet(currentQueued, currentQueued)) {
									// log("...but we have space left in the queue")
									Some(inProgress)
								} else {
									tryUpdate()
								}
							}
						}
					}
					case (completed, inProgress) => {
						// log("we have some completed async items")
						advanceQueued(completed.length + completedSync)
						Some(inProgress)
					}
				}
			}
			tryUpdate()
		}

		private final def advanceNode(
			headNode: Node[EnqueueableTask],
			node: Node[EnqueueableTask],
			inProgress: List[Future[_]]): Node[EnqueueableTask] =
		{
			// val log = Log.id("advanceNode")
			// var logNullifyTailAttempts = 0
			val waitTime = 100
			while (node.next == null) {
				// looks like we've hit the tail.
				// logNullifyTailAttempts += 1
				// if(logNullifyTailAttempts>5) log("tail nullify attempts: " + logNullifyTailAttempts)

				// The enqueueing thread of any final task will eventually
				// update `queued` to include it (since there must be capacity).
				// To prevent an inconsistent view of the world in `doEnqueue`,
				// don't break this chain until queued matches tail.
				val currentQueued = queued.get
				if (currentQueued.node.eq(node)) {
					// We need to save storedInProgress, as the next run could
					// occur the moment we set `headNode` to null
					storedInProgress = inProgress
					if (tail.compareAndSet(node, null)) {
						// log(s"completed last item (${System.identityHashCode(node)}); storedInProgress has ${inProgress.length}")
						head.compareAndSet(headNode, null) // best effort; helps with GC
						return null
					}
				}
				// We've interrupted an enqueuer thread -- either
				// node.next is about to be populated, or queued still needs to
				// be updated to include the final node in the chain.
				//
				// Either way, we just wait until that thread has progressed and try again.
				LockSupport.parkNanos(waitTime)
			}
			// log(s"advanced")
			node.next
		}

		private final def advanceQueued(n: Int): Unit =
		{
			// n is always >0
			// val log = Log.id(s"advanceQueued($n)")
			// var logUpdatedCount = 0
			@tailrec def tryUpdate(numCompleted: Int): Unit = {
				// logUpdatedCount += 1
				// if (logUpdatedCount > 5) log("runner queued update count: " + logUpdatedCount)
				val currentQueued = queued.get
				if (currentQueued.len == bufLen) {
					// we're at capacity, only this thread can advance it (just use `set`)
					val queuedNode = currentQueued.node
					// log(s"at capacity (queued = ${System.identityHashCode(queuedNode)}")

					val nextQueued = queuedNode.next
					if (nextQueued == null) {
						// log(s"no next; decrementing count to ${bufLen - numCompleted}")
						val newQueued = new Queued(bufLen - numCompleted, queuedNode)
						queued.update(newQueued)
						val updatedNext = queuedNode.next
						if (updatedNext != null) {
							// race: we thought there was no next item, but it's been made
							// non-null since we checked. We now have a `.next` node which
							// is marked as not enqueued, but we have capacity for it.
	
							// log("queuedNode.next was just modified, it may not have seen the free capacity")
							if (queued.compareAndSet(newQueued, newQueued.add(updatedNext))) {
								// log(" - it is now enqueued")
								updatedNext.item.enqueuedAsync()
							} else {
								// the above condition is self-righting if another enqueue() happens
								// log(" - nevermind; queued has been advanced already")
							}
						}
						// if node.next was still null, then any future enqueues must not yet
						// have checked `queued`; they will see the spare capacity
					} else {
						// log(s"dequeueing next item (${System.identityHashCode(nextQueued)})")
						queued.set(new Queued(bufLen, nextQueued))
						// notify the lucky winner
						nextQueued.item.enqueuedAsync()
						val completedRemaining = numCompleted - 1
						if (completedRemaining > 0) {
							// log(s"completing another $completedRemaining")
							tryUpdate(completedRemaining)
						}
					}
				} else {
					// We're not at capacity. If threads enqueue more tasks in the meantime
					// this CAS will fail and we'll loop again
					if (!queued.compareAndSet(currentQueued, currentQueued)) {
						tryUpdate(numCompleted)
					}
				}
			}
			tryUpdate(n)
		}
	}
}
