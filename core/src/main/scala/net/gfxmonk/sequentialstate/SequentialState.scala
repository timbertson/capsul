package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

/** A wrapper for getting / setting state */
class Ref[T](init:T) {
	@volatile private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}

object SequentialState {
	/** Create a SequentialState with the default bufLen */
	def apply[T](v: T)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, SequentialExecutor())

	/** Create a SequentialState with the provided bufLen */
	def apply[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, SequentialExecutor(bufLen))

	def apply[T](v: T, thread: SequentialExecutor)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, thread)
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
function, but you may not do so asynchronously (e.g. when your future
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
class SequentialState[T](init: T, thread: SequentialExecutor) {
	private val state = new Ref(init)

	/** Send a pure transformation */
	def sendTransform(fn: T => T): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(fn(state.current))))

	/** Send a set operation */
	def sendSet(updated: T): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(updated)))

	/** Send an access operation */
	def sendAccess(fn: T => _): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => fn(state.current)))

	/** Send an access operation which returns a [[StagedFuture]][R] */
	def sendAccessStaged[A](fn: T => StagedFuture[A])(implicit ec: ExecutionContext): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnlyStaged(() => fn(state.current)))

	/** Send an access operation which returns a [[Future]][R] */
	def sendAccessAsync[A](fn: T => StagedFuture[A]): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnlyAsync(() => fn(state.current)))

	/** Return the current state value */
	def current: Future[T] =
		thread.enqueue(UnitOfWork.Full(() => state.current))

	/** Perform a full mutation */
	def mutate[R](fn: Ref[T] => R): StagedFuture[R] =
		thread.enqueue(UnitOfWork.Full(() => fn(state)))

	/** Perform a pure transformation */
	def transform(fn: T => T): StagedFuture[T] =
		thread.enqueue(UnitOfWork.Full { () =>
			val updated = fn(state.current)
			state.set(updated)
			updated
		})

	/** Perform a function with the current state */
	def access[R](fn: T => R): StagedFuture[R] =
		thread.enqueue(UnitOfWork.Full(() => fn(state.current)))

	/** Perform a mutation which returns a [[StagedFuture]][R] */
	def mutateStaged[R](fn: Ref[T] => StagedFuture[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullStaged[R](() => fn(state)))

	/** Perform a mutation which returns a [[Future]][R] */
	def mutateAsync[R](fn: Ref[T] => Future[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullAsync[R](() => fn(state)))

	/** Perform an access which returns a [[StagedFuture]][R] */
	def accessStaged[R](fn: T => StagedFuture[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullStaged[R](() => fn(state.current)))

	/** Perform an access which returns a [[Future]][R] */
	def accessAsync[R](fn: T => Future[R])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullAsync[R](() => fn(state.current)))
}
