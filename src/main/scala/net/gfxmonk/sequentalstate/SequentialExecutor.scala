package net.gfxmonk.sequentialstate

import java.util.concurrent.ConcurrentLinkedQueue

import monix.execution.atomic.{Atomic, AtomicInt}

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
	private val successfulUnit = Future.successful(())
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = AtomicInt(ExecutorState.empty)
	private val tasks = new ConcurrentLinkedQueue[UnitOfWork[_]]()

	val workLoop:Runnable = new Runnable() {
		def run() {
			@tailrec def loop(maxIterations: Int, numEnqueued: Int): Unit = {
//				println("loop go")
				// numEnqueued may be negative, which equates to 0
				val it = tasks.iterator()
				var tasksConsumed = 0

				while(it.hasNext) {
					val task = it.next()
					tasks.poll() // remove the item we just processed
					tasksConsumed += 1
//					println("loop run")
					task.run()

					// TODO: is this accurate?
					if (tasksConsumed > numEnqueued) {
						task.enqueuedAsync()
					}
				}

//				var tasksToEnqueue = if (tasksConsumed < bufLen) {
//					// enqueue `tasksConsumed`, after advancing up to `bufLen`
//					var skip = bufLen - tasksConsumed
//					while(skip > 0 && it.hasNext) {
//						skip -= 1
//						it.next().enqueuedAsync()
//					}
//					tasksConsumed
//				} else {
//					// enqueue `bufLen`, immediately
//					bufLen
//				}
//
//				while (tasksToEnqueue > 0 && it.hasNext) {
//					tasksToEnqueue -= 1
//					it.next().enqueuedAsync()
//				}

				if (tasksConsumed >= 0) {
					// TODO: consistency!
//					Thread.sleep(10)
					val newState = state.transformAndGet(_ - tasksConsumed)
//					println("popped " + tasksConsumed + " tasks, now " + newState)
					if (ExecutorState.hasTasks(newState)) {
						// we got more tasks, don't stop now!
						// Note that only the first `bufLen` tasks will have been preemptively marked as enqueued
						val numEnqueuedTasks = bufLen - tasksConsumed
						loop(maxIterations, numEnqueuedTasks)
					}
				}

				// TODO: implement maxIterations
			}
			loop(200, bufLen)
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

	private def enqueue[A](task: UnitOfWork[A]): Boolean = {
		val prevState = state.getAndIncrement()
		tasks.add(task)

//		println("after enqueue, " + prevState + ", " + prevState.hasSpace(bufLen))
		if(ExecutorState.isEmpty(prevState)) {
			//				println("Running!")
			ec.execute(workLoop)
			true
		} else {
			ExecutorState.hasSpace(prevState, bufLen)
		}
	}
}

object ExecutorState {
	type ExecutorState = Int
	def empty: ExecutorState = 0
	def hasTasks(state: ExecutorState) = state != 0
	def isEmpty(state: ExecutorState) = state == 0
	def hasSpace(state: ExecutorState, capacity: Int) = state < capacity
}
