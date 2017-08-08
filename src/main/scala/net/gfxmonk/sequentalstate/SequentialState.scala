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
	// Naming:
	//
	//  - send*:    Enqueue an action, returning Future[Unit]. Once the future is resolved, the
	//              action has been accepted by the worker, but not yet performed.
	//
	//  - *:        Perform an action and return its result. The future resolves once
	//              the action is complete.
	//
	//  - raw*:     Perform an action, returning a Future[Future[T]]. The outer future
	//              resolves once the task is enqueued (at which point futher tasks may
	//              be enqueued). The inner future resolves once the task is completed.
	//
	// Action types:
	//
	// mutate(fn: Function[State[T],R]
	// transform(fn: Function[T,T])
	// set(value: T)
	// access: Function[T,R] -> accepts a mutable Ref for both reading and setting.

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

	def sendAccessFuture[A](fn: Function[T,StagedFuture[A]]): Future[Unit] =
		thread.enqueueOnly(UnitOfWork.EnqueueOnlyAsync(() => fn(state.current)))

	// == await* ==

	def awaitMutate[R](fn: Function[Ref[T],R]): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => fn(state)))

	def awaitMutateStaged[R](fn: Function[Ref[T],StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyStaged(() => fn(state)))

	def awaitTransform[R](fn: Function[T,T]): Future[Unit] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => state.set(fn(state.current))))

	def awaitSet[R](updated: T): Future[Unit] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => state.set(updated)))

	def awaitAccess[R](fn: Function[T,R]): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => fn(state.current)))

	def awaitAccessStaged[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyStaged(() => fn(state.current)))

	def awaitAccessAsync[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): Future[R] =
		thread.enqueueReturn(UnitOfWork.ReturnOnlyAsync(() => fn(state.current)))

	def current: Future[T] =
		thread.enqueueReturn(UnitOfWork.ReturnOnly(() => state.current))

	// == raw* ==

	def rawMutate[R](fn: Function[Ref[T],R]): StagedFuture[R] =
		thread.enqueueRaw(UnitOfWork.Full(() => fn(state)))

	def rawAccess[R](fn: Function[T,R]): StagedFuture[R] =
		thread.enqueueRaw(UnitOfWork.Full(() => fn(state.current)))

	def rawMutateStaged[R](fn: Function[Ref[T],StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullStaged[R](() => fn(state)))

	def rawMutateAsync[R](fn: Function[Ref[T],Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullAsync[R](() => fn(state)))

	def rawAccessStaged[R](fn: Function[T,StagedFuture[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullStaged[R](() => fn(state.current)))

	def rawAccessAsync[R](fn: Function[T,Future[R]])(implicit ec: ExecutionContext): StagedFuture[R] =
		thread.enqueueRaw(new UnitOfWork.FullAsync[R](() => fn(state.current)))
}
