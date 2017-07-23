package net.gfxmonk.sequentialstate

import monix.execution.atomic.Atomic
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}

object SequentialExecutor {
	val defaultBufferSize = 10
	def apply(bufLen: Int = defaultBufferSize)(implicit ec: ExecutionContext) = new SequentialExecutor(bufLen)
}

class SequentialExecutor(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = Atomic(ExecutorState.empty)

	val workLoop:Runnable = new Runnable() {
		def run() {
			@tailrec def loop(maxIterations: Int): Unit = {
				val oldState = state.getAndTransform(ExecutorState.popTask)
				if (oldState.hasTasks) {
					// we popped a task!
					oldState.tasks.head.run()
					if (maxIterations == 0) {
						// re-enqueue the runnable instead of looping to prevent starvation
						ec.execute(workLoop)
					} else {
						loop(maxIterations - 1)
					}
				}
			}
			loop(200)
		}
	}

	def enqueueOnly[R](fun: Function0[R]): Future[Unit] = {
		enqueueAsync(fun).map((_:Future[R]) => ())
	}

	def enqueueReturn[R](fun: Function0[R]): Future[R] = {
		enqueueAsync(fun).flatMap(identity)
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork(fun, bufLen)
		if (enqueue(task)) {
			Future.successful(task.resultPromise.future)
		} else {
			task.enqueuedPromise.future
		}
	}

	private def enqueue[A](task: UnitOfWork[A]): Boolean = {
		val prevState = state.getAndTransform(task.tryEnqueue)
		if (prevState.hasSpace(bufLen)) {
			if(!prevState.running) {
				ec.execute(workLoop)
			}
			true
		} else {
			prevState.nextWaiter.onComplete { _ =>
				if (enqueue(task)) {
					task.enqueuedAsync()
				}
			}
			false
		}
	}
}


object ExecutorState {
	def popTask(state: ExecutorState):ExecutorState = {
		if (!state.hasTasks) {
			state.park()
		} else {
			val numTasks = state.numTasks - 1
			val numWaiters = if (state.hasWaiters) {
				state.numWaiters - 1
			} else {
				0
			}
			new ExecutorState(state.tasks.tail, state.running, numTasks, numWaiters)
		}
	}

	def empty = new ExecutorState(Queue.empty[UnitOfWork[_]], false, 0, 0)
}

class ExecutorState(
	val tasks: Queue[UnitOfWork[_]],
	val running: Boolean,
	private val numTasks: Int,
	private val numWaiters: Int
) {
	def hasTasks = numTasks != 0
	def hasWaiters = numWaiters != 0
	def hasSpace(capacity: Int) = numTasks < capacity

	def nextWaiter = {
		val taskIdx = numWaiters % numTasks
		tasks(taskIdx).resultPromise.future
	}

	def enqueueTask(task: UnitOfWork[_]): ExecutorState = {
//		assert(numWaiters == 0)
		new ExecutorState(tasks.enqueue(task), true, numTasks + 1, numWaiters)
	}

	def enqueueWaiter(): ExecutorState = {
//		assert(running)
		new ExecutorState(tasks, running, numTasks, numWaiters + 1)
	}

	def park() = {
		// assert(running)
		new ExecutorState(tasks, false, numTasks, numWaiters)
	}
}
