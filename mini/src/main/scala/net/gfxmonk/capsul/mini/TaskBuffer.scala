package net.gfxmonk.capsul.mini

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.LockSupport

import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

private [capsul] object State {
	// State is stored as a uint64.
	// High bytes are for running tasks, low bytes are
	// for pending tasks.

	// just for testing / debugging
	def repr(t:State) = {
		(numRunning(t), numQueued(t))
	}
	type State = Long
	def ref(n: State): AtomicLong = AtomicLong(n)

	private val QUEUED_OFFSET = 32
	private val QUEUED_MASK = 0xffffffffL // 4 bytes (32 bits)
	def make(running: Long, queued: Long) = {
		(running << QUEUED_OFFSET) | queued
	}
	def numRunning(t:State):Long = t >>> QUEUED_OFFSET
	def numQueued(t:State):Long = t & QUEUED_MASK
}

class TaskBuffer(capacity: Int, ec: ExecutionContext) {
	def enqueue[T](f: Function0[Future[T]]): Future[Unit] = enqueueOnly(f)
	def result[T](f: Function0[Future[T]]): Future[T] = enqueueResult(f).flatten
	def run[T](f: Function0[Future[T]]): Future[Future[T]] = enqueueResult(f)

	private var stateRef = State.ref(State.make(0,0))
	private val pending = new ConcurrentLinkedQueue[AsyncWork]()

	private def enqueueOnly[T](task: Function0[Future[T]]): Future[Unit] = {
		if (doEnqueue(task)) {
			// run immediately
			AsyncWorkIgnore.runAsync(task, didCompleteWork, ec)
			Future.unit
		} else {
			// over capacity, enqueue
			val work = new AsyncWorkIgnore[T](task)
			pending.add(work)
			work.enqueuedPromise.future
		}
	}

	private def enqueueResult[T](task: Function0[Future[T]]): Future[Future[T]] = {
		val promise = Promise[T]()
		if (doEnqueue(task)) {
			AsyncWorkReturn.runAsync(task, promise, didCompleteWork, ec)
			Future.successful(promise.future)
		} else {
			// over capacity, enqueue
			val work = new AsyncWorkReturn[T](task, promise)
			pending.add(work)
			work.enqueuedPromise.future
		}
	}

	private def doEnqueue[T](task: Function0[Future[T]]): Boolean = {
		val prevState = stateRef.getAndTransform { state =>
			val running = State.numRunning(state)
			val queued = State.numQueued(state)
			if (running < capacity) {
				// assert queued == 0
				State.make(running+1, queued)
			} else {
				State.make(running, queued+1)
			}
		}
		State.numRunning(prevState) < capacity
	}

	private def didCompleteWork() {
		val prevState = stateRef.getAndTransform { state =>
			val running = State.numRunning(state)
			val queued = State.numQueued(state)
			if (queued > 0) {
				State.make(running, queued-1)
			} else {
				State.make(running-1, queued)
			}
		}
		if (State.numQueued(prevState) > 0) {
			// there will be something in the queue,
			// but we might have to wait for the enqueue thread
			@tailrec
			def dequeue() {
				val work = pending.poll()
				if (work == null) {
					LockSupport.parkNanos(0)
					dequeue()
				} else {
					work.runAsync(didCompleteWork, ec)
				}
			}
			dequeue()
		}
	}
}

object TaskBuffer {
	def apply(capacity: Int)(implicit ec: ExecutionContext) = new TaskBuffer(capacity, ec)
}
