package net.gfxmonk.capsul.mini

import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicLong
import java.util

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

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
		thread.enqueueOnly(new Tell(() => state.set(fn(state.current))))

	/** Send a set operation */
	def sendSet(updated: T): Unit =
		thread.enqueueOnly(new Tell(() => state.set(updated)))

	/** Send an access operation */
	def sendAccess(fn: T => _): Unit =
		thread.enqueueOnly(new Tell(() => fn(state.current)))

	/** Return the current state value */
	def current: Future[T] =
		thread.enqueue(new Ask(() => state.current))

	/** Perform a full mutation */
	def mutate[R](fn: Ref[T] => R): Future[R] =
		thread.enqueue(new Ask(() => fn(state)))

	/** Perform a pure transformation */
	def transform(fn: T => T): Future[T] =
		thread.enqueue(new Ask(() => {
			val updated = fn(state.current)
			state.set(updated)
			updated
		}))

	/** Perform a function with the current state */
	def access[R](fn: T => R): Future[R] =
		thread.enqueue(new Ask(() => fn(state.current)))
}


private[capsul] trait EnqueueableTask {
	def run(): Unit
}

private [capsul] case class Tell[A](fn: Function0[A]) extends EnqueueableTask {
	final override def run(): Unit = {
		try {
			fn()
		} catch {
			case NonFatal(error) => {
				Console.err.println(s"Uncaught error in enqueued task: $error")
				error.printStackTrace()
			}
		}
	}
}

private [capsul] case class Ask[A](fn: Function0[A]) extends EnqueueableTask {
	val resultPromise: Promise[A] = Promise[A]()
	final override def run(): Unit = {
		try {
			resultPromise.success(fn())
		} catch {
			case NonFatal(error) => {
				resultPromise.failure(error)
			}
		}
	}
}

object SequentialExecutor {
	private [capsul] def unbounded()(implicit ec: ExecutionContext) =
		new SequentialExecutor(new ConcurrentLinkedQueue[EnqueueableTask]())

	private [capsul] def bounded(bufLen: Int)(implicit ec: ExecutionContext) =
		new SequentialExecutor(new LinkedBlockingQueue[EnqueueableTask](bufLen))
}

class SequentialExecutor(queue: util.Queue[EnqueueableTask])(implicit ec: ExecutionContext) {
	private [capsul] val stateRef = new AtomicLong(0)

	def enqueueOnly[R](task: Tell[_]): Unit = {
		doEnqueue(task)
	}

	def enqueue[R](task: Ask[R]): Future[R] = {
		doEnqueue(task)
		task.resultPromise.future
	}

	private def doEnqueue(work: EnqueueableTask) {
		queue.add(work)
		val state = stateRef.getAndIncrement()
		// if state was 0, run loop
		if (state == 0) {
			ec.execute(workLoop)
		}
	}

	// A runnable which reepeatedly consumes & runs items in the queue until it's empty
	val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			loop(1000)
		}

		@tailrec
		private def loop(_maxItems: Int): Unit = {
			var itemsConsumed = 0
			var maxItems = _maxItems

			var item = queue.poll()

			while (item != null) {
				itemsConsumed += 1
				maxItems -= 1
				item.run()
				if (maxItems <= 0) {
					val newItems = stateRef.addAndGet(-1 * itemsConsumed)
					itemsConsumed = 0
					if (newItems > 0) {
						// definitely new items; trampoline
						return ec.execute(workLoop)
					}
					// else may or may not be new items, keep polling
				}
				item = queue.poll()
			}

			val newItems = stateRef.addAndGet(-1 * itemsConsumed)
			if (newItems > 0) {
				// there's more work now, keep going
				return loop(maxItems)
			}
		}
	}
}
