package net.gfxmonk.sequentialstate

import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

class Ref[T](init:T) {
	@volatile private var v = init
	def current = v
	def set(updated:T) {
		v = updated
	}
}

object SequentialState {
	def apply[T](v: T)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, SequentialExecutor())

	def apply[T](v: T, bufLen: Int)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, SequentialExecutor(bufLen))

	def apply[T](v: T, thread: SequentialExecutor)(implicit ec: ExecutionContext) =
		new SequentialState[T](v, thread)
}

class SequentialState[T](init: T, thread: SequentialExecutor) {
	private val state = new Ref(init)
	// Methods are named <dispatch><operation><mode>
	//
	// Dispatch:
	//
	//  - send:    Enqueue an action, returning Future[Unit]. Once the future is resolved, the
	//             action has been accepted by the worker, but not necessarily performed.
	//
	//  - (none):  Perform an action, returning a StagedFuture[T].
	//             StagedFuture is a Future[T] with the additional ability to
	//             determine when the item has been accepted, but not yet completed.
	//             This can be used to manage backpressure.
	//
	// Operations:
	//
	//  - mutate:    accepts a function of type Ref[T] => R, returns R
	//  - transform  accepts a function of type T => T, returns Unit
	//  - set        accepts a new value of type T, returns Unit
	//  - access     accepts a function of type T => R, returns R.
	//               The function is executed sequentually with other tasks,
	//               so it's safe to mutate `T`. If you simply want to get
	//               the current vaue without blocking other tasks, use `current`.
	//
	// Modes:
	//
	// Modes control when the state will accept more work.
	// Each state will accept up to <bufLen> items immediately, and
	// will run the functions sequentually.
	//
	// For asynchronous modes, subsequent tasks will still be run immediately
	// after the function completes, but the task won't be considered done
	// (for the purposes of accepting more tasks) until the future is resolved.
	//
	// This means that you must be careful with `mutate` actions or with mutable
	// state objects - you may mutate the state during the execution of the
	// function, but you may not do so asynchronously (e.g. when your future
	// completes)
	//
	//  - (empty): Synchronous. The task is completed as soon as it returns.
	//  - async:   async (Future). The work returns a Future[T], and the next
	//             task may begin immediately. This task continues to occupy a
	//             slot in this state's queue until the future completes.
	//  - staged:  async (StagedFuture). The work returns a StagedFuture[T], and
	//             the next task may begin immediately. This task continues to
	//             occupy a slot in this state's queue until the StagedFuture
	//             is accepted.

	def sendTransform(fn: Function[T,T]): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(fn(state.current))))

	def sendSet(updated: T): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => state.set(updated)))

	def sendAccess(fn: Function[T,_]): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnly(() => fn(state.current)))

	def sendAccessStaged[A](fn: Function[T,StagedFuture[A]])(implicit ec: ExecutionContext): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnlyStaged(() => fn(state.current)))

	def sendAccessAsync[A](fn: Function[T,StagedFuture[A]]): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnlyAsync(() => fn(state.current)))

	def current: Future[T] =
		thread.enqueue(UnitOfWork.Full(() => state.current))

	def mutate[R](fn: Function[Ref[T],R]): StagedFuture[R] =
		thread.enqueue(UnitOfWork.Full(() => fn(state)))

	def access[R](fn: Function[T,R]): StagedFuture[R] =
		thread.enqueue(UnitOfWork.Full(() => fn(state.current)))

	def mutateStaged[R](fn: Function[Ref[T],StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullStaged[R](() => fn(state)))

	def mutateAsync[R](fn: Function[Ref[T],Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullAsync[R](() => fn(state)))

	def accessStaged[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullStaged[R](() => fn(state.current)))

	def accessAsync[R](fn: Function[T,Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueue(new UnitOfWork.FullAsync[R](() => fn(state.current)))
}
