package net.gfxmonk.capsul.mini

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}

import net.gfxmonk.capsul.{AtomicWork, HasResultPromise}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/** A wrapper for getting / setting state */
class Ref[T](init:T) {
	private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}

object Capsul {
	/** Create a Capsul with an unbounded queue */
	def apply[T](v: T)(implicit ec: ExecutionContext) =
		new Capsul(v, SequentialExecutor.unbounded())

	/** Create a Capsul which a bounded queue. Adding new work will block while there are `bufLen` pending items. */
	def bounded[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) =
		new Capsul(v, SequentialExecutor.bounded(bufLen))
}

/**
An encapsulation for thread-safe state.

This package (net.gfxmonk.capsul.mini) contains a simplified version of the main Capsul functionality,
which does not support backpressure.

Methods are named <dispatch><operation>

== Dispatch types: ==

 - '''send''':   Enqueue an action, returning [[Unit]]. Enqueued items will be executed in the
								 order they are enqueued.

 - '''(none)''': Perform an action, returning a [[Future]][T] which completes with the action's result.

== Operations: ==

 - '''mutate''':    accepts a function of type `[[Ref]][T] => R`, returns `R`
 - '''transform''': accepts a function of type `T => T`, returns [[Unit]]
 - '''set''':       accepts a new value of type `T`, returns [[Unit]]
 - '''access''':    accepts a function of type `T => R`, returns `R`.
										The function is executed sequentually with other tasks,
										so it's safe to mutate `T`. If you simply want to get
										the current vaue without blocking other tasks, use [[current]].

*/
class Capsul[T](init: T, thread: SequentialExecutor) {
	private val state = new Ref(init)

	/** Send a pure transformation */
	def sendTransform(fn: T => T): Unit =
		thread.enqueueOnly(new AtomicWork.EnqueueOnly(() => state.set(fn(state.current))))

	/** Send a set operation */
	def sendSet(updated: T): Unit =
		thread.enqueueOnly(new AtomicWork.EnqueueOnly(() => state.set(updated)))

	/** Send an access operation */
	def sendAccess(fn: T => _): Unit =
		thread.enqueueOnly(new AtomicWork.EnqueueOnly(() => fn(state.current)))

	/** Return the current state value */
	def current: Future[T] =
		thread.enqueue(new AtomicWork.Full(() => state.current))

	/** Perform a full mutation */
	def mutate[R](fn: Ref[T] => R): Future[R] =
		thread.enqueue(new AtomicWork.Full(() => fn(state)))

	/** Perform a pure transformation */
	def transform(fn: T => T): Future[T] =
		thread.enqueue(new AtomicWork.Full(() => {
			val updated = fn(state.current)
			state.set(updated)
			updated
		}))

	/** Perform a function with the current state */
	def access[R](fn: T => R): Future[R] =
		thread.enqueue(new AtomicWork.Full(() => fn(state.current)))
}

object SequentialExecutor {
	private [capsul] def unbounded()(implicit ec: ExecutionContext) =
		new SequentialExecutor(new ConcurrentLinkedQueue[AtomicWork]())

	private [capsul] def bounded(bufLen: Int)(implicit ec: ExecutionContext) =
		new SequentialExecutor(new LinkedBlockingQueue[AtomicWork](bufLen))
}

class SequentialExecutor(queue: util.Queue[AtomicWork])(implicit ec: ExecutionContext) {
	private [capsul] val stateRef = new AtomicLong(0)

	def enqueueOnly[R](task: AtomicWork): Unit = {
		doEnqueue(task)
	}

	def enqueue[R](task: AtomicWork with HasResultPromise[R]): Future[R] = {
		doEnqueue(task)
		task.resultPromise.future
	}

	private def doEnqueue(work: AtomicWork) {
		val prevState = stateRef.getAndIncrement()
		queue.add(work)
		// if state was 0, run loop
		if (prevState == 0) {
			ec.execute(workLoop)
		}
	}

	// A runnable which repeatedly consumes & runs items in the queue until it's empty
	private [capsul] val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			loop(1000, 0, stateRef.get)
		}

		@tailrec
		private def waitForWork(): AtomicWork = {
			val item = queue.poll()
			if (item == null) {
				waitForWork()
			} else {
				item
			}
		}

		private def tryShutdown(itemsConsumed: Long): Long = {
			stateRef.addAndGet(-1 * itemsConsumed)
		}

		@tailrec
		private def loop(maxItems: Int, itemsConsumed: Int, availableItems: Long): Unit = {
			if (maxItems == 0) {
				// trampoline to prevent thread starvation
				if (tryShutdown(itemsConsumed) > 0) {
					// available work remains above zero,
					// trampoline and keep running
					ec.execute(workLoop)
				}
			} else  if (itemsConsumed == availableItems) {
				// we've completed as much work as we know about,
				// either pick up more or stop
				val remainingItems = tryShutdown(itemsConsumed)
				if (remainingItems > 0) {
					// there's more work now, keep going
					loop(maxItems, 0, remainingItems)
				}
			} else {
				// otherwise, pick the next task and run it
				val item = waitForWork()
				item.run()
				loop(maxItems-1, itemsConsumed+1, availableItems)
			}
		}
	}
}
