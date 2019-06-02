package net.gfxmonk.capsul

import net.gfxmonk.capsul.StagedWork.HasEnqueuePromise

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
	/** Create a Capsul with the default bufLen */
	def apply[T](v: T)(implicit ec: ExecutionContext) =
		new Capsul[T](v, SequentialExecutor())

	/** Create a Capsul with the provided bufLen */
	def apply[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) =
		new Capsul[T](v, SequentialExecutor(bufLen))

	def apply[T](v: T, thread: CapsulExecutor)(implicit ec: ExecutionContext) =
		new Capsul[T](v, thread)
}

/**
An encapsulation for thread-safe state.

Methods are named <dispatch><operation><mode>

== Dispatch types: ==

 - '''send''':   Enqueue an action, returning [[Future]][Unit]. Once the future is resolved, the
                 action has been accepted by the worker, but not necessarily performed.

 - '''(none)''': Perform an action, returning a [[StagedFuture]][T].

== Operations: ==

 - '''mutate''':    accepts a function of type `[[Ref]][T] => R`, returns `R`
 - '''transform''': accepts a function of type `T => T`, returns [[Unit]]
 - '''set''':       accepts a new value of type `T`, returns [[Unit]]
 - '''access''':    accepts a function of type `T => R`, returns `R`.
                    The function is executed sequentually with other tasks,
                    so it's safe to mutate `T`. If you simply want to get
                    the current vaue without blocking other tasks, use [[current]].

== Modes: ==

Modes control when the state will accept more work.
Each state will accept up to `bufLen` items immediately, and
will run the functions sequentually.

For asynchronous modes, subsequent tasks will still be run immediately
after the function completes, but the task won't be considered done
(for the purposes of accepting more tasks) until the future is resolved.

This means that you must be careful with `mutate` actions or with mutable
state objects - you may mutate the state during the execution of the
function, but you may not do so asynchronously (e.g. after your future
completes)

 - '''(empty)''': Synchronous. The task is completed as soon as it returns.
 - '''async''':   async ([[Future]]). The work returns a [[Future]][T], and the next
                  task may begin immediately. This task continues to occupy a
                  slot in this state's queue until the future completes.
 - '''staged''':  async ([[StagedFuture]]). The work returns a [[StagedFuture]][T], and
                  the next task may begin immediately. This task continues to
                  occupy a slot in this state's queue until the StagedFuture
                  is accepted.

*/
class Capsul[T](init: T, thread: CapsulExecutor) {
	private val state = new Ref(init)

	/** Send a pure transformation */
	def sendTransform(fn: T => T): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(() => state.set(fn(state.current))))

	/** Send a set operation */
	def sendSet(updated: T): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(() => state.set(updated)))

	/** Send an access operation */
	def sendAccess[R](fn: T => R): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(() => fn(state.current)))

	/** Send an access operation which returns a [[Future]][R] */
	def sendAccessAsync[A](fn: T => Future[A]): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(() => fn(state.current)))

	/** Send a mutate operation which returns a [[Future]][R] */
	def sendMutateAsync[A](fn: Ref[T] => Future[A]): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(() => fn(state)))

	/** Return the current state value */
	def current: Future[T] =
		thread.enqueue(StagedWork.Full(() => state.current)).flatten

	/** Perform a full mutation */
	def mutate[R](fn: Ref[T] => R): Future[Future[R]] =
		thread.enqueue(StagedWork.Full(() => fn(state)))

	/** Perform a pure transformation */
	def transform(fn: T => T): Future[Future[T]] =
		thread.enqueue(StagedWork.Full { () =>
			val updated = fn(state.current)
			state.set(updated)
			updated
		})

	/** Perform a function with the current state */
	def access[R](fn: T => R): Future[Future[R]] =
		thread.enqueue(StagedWork.Full(() => fn(state.current)))

	/** Perform an access which returns a [[Future]][R] */
	def accessAsync[R](fn: T => Future[R])(implicit ec: ExecutionContext): Future[Future[R]] =
		thread.enqueue(new StagedWork.FullAsync[R](() => fn(state.current)))
}

// like `Capsul`, but without
class StatelessCapsul(val thread: CapsulExecutor) extends AnyVal {
	def send[T](fn: Function0[T]): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnly(fn))

	def sendAsync[T](fn: Function0[Future[T]])(implicit ec: ExecutionContext): Future[Unit] =
		thread.enqueueOnly(StagedWork.EnqueueOnlyAsync(fn))

	def run[T](fn: Function0[T]): StagedFuture[T] =
		thread.enqueue(StagedWork.Full(fn))

	def runAsync[T](fn: Function0[Future[T]])(implicit ec: ExecutionContext): StagedFuture[T] =
		thread.enqueue(StagedWork.FullAsync(fn))
}

trait CapsulExecutor {
	def enqueueOnly[R](task: StagedWork with HasEnqueuePromise[Unit]): Future[Unit]

	def enqueue[R](task: StagedWork
			with StagedWork.HasEnqueuePromise[Future[R]]
			with HasResultPromise[R]
	): Future[Future[R]]
}
