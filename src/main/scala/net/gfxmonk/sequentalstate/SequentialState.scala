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
	// Methods are named <result><operation><mode>
	//
	// Results:
	//
	//  - send:    Enqueue an action, returning Future[Unit]. Once the future is resolved, the
	//             action has been accepted by the worker, but not yet performed.
	//
	//  - await:   Perform an action and return its result. The future resolves once
	//             the action is complete.
	//
	//  - (empty): Perform an action, returning a Future[Future[T]]. The outer future
	//             resolves once the task is enqueued (at which point futher tasks may
	//             be enqueued). The inner future resolves once the task is completed.
	//
	// Actions:
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
	// (for the purposes of enqueueing more tasks) until the future is resolved.
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

	// == send* ==
	//
	// sendMutate: omitted; sendTransform is just as functional
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

	// == await* ==

	def awaitMutate[R](fn: Function[Ref[T],R]): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => fn(state)))

	def awaitMutateStaged[R](fn: Function[Ref[T],StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyStaged(() => fn(state)))

	def awaitMutateAsync[R](fn: Function[Ref[T],Future[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyAsync(() => fn(state)))

	def awaitTransform[R](fn: Function[T,T]): Future[Unit] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => state.set(fn(state.current))))

	def awaitAccess[R](fn: Function[T,R]): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => fn(state.current)))

	def awaitAccessStaged[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyStaged(() => fn(state.current)))

	def awaitAccessAsync[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyAsync(() => fn(state.current)))

	def current: Future[T] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => state.current))

	// == raw* ==

	def mutate[R](fn: Function[Ref[T],R]): StagedFuture[R] =
		thread.enqueueRaw(UnitOfWork.Full(() => fn(state)))

	def access[R](fn: Function[T,R]): StagedFuture[R] =
		thread.enqueueRaw(UnitOfWork.Full(() => fn(state.current)))

	def mutateStaged[R](fn: Function[Ref[T],StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullStaged[R](() => fn(state)))

	def mutateAsync[R](fn: Function[Ref[T],Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullAsync[R](() => fn(state)))

	def accessStaged[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullStaged[R](() => fn(state.current)))

	def accessAsync[R](fn: Function[T,Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullAsync[R](() => fn(state.current)))
}
