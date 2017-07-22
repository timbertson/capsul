import java.util.concurrent.locks.LockSupport

import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

object ThreadState {
	def popTask(state: ThreadState):ThreadState = {
		if (!state.hasTasks) {
			state.park()
		} else {
			val numTasks = state.numTasks - 1
			val numWaiters = Math.max(state.numWaiters - 1, 0)
      new ThreadState(state.tasks.tail, state.running, numTasks, numWaiters)
		}
	}

	def empty = new ThreadState(Queue.empty[UnitOfWork[_]], false, 0, 0)
}

class ThreadState(val tasks: Queue[UnitOfWork[_]], val running: Boolean, val numTasks: Int, val numWaiters: Int) {
	def hasTasks = numTasks != 0
	def hasWaiters = numWaiters != 0
	def hasSpace(capacity: Int) = numTasks < capacity

	def nextWaiter = {
    val taskIdx = numWaiters % numTasks
    tasks(taskIdx).resultPromise.future
	}

	def enqueueTask(task: UnitOfWork[_]): ThreadState = {
//		assert(numWaiters == 0)
		new ThreadState(tasks.enqueue(task), true, numTasks + 1, numWaiters)
	}

	def enqueueWaiter(): ThreadState = {
//		assert(running)
		new ThreadState(tasks, running, numTasks, numWaiters + 1)
	}

	def park() = {
		// assert(running)
		new ThreadState(tasks, false, numTasks, numWaiters)
	}
}
