package net.gfxmonk.capsul

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import net.gfxmonk.capsul.internal.Log
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future}

import UnitOfWork._

object SimpleExecutor {
	val DEFAULT_LIMIT = 10
	type State = Long
	val TASK_MASK = 0xFFFFFFFFL // 4 lower bytes
	val FUTURE_SHIFT = 32 // 4 bytes
	val FUTURE_MASK = TASK_MASK << FUTURE_SHIFT // 4 top bytes
	val SINGLE_FUTURE = 0x01L << FUTURE_SHIFT // lower bit of FUTURE_MASK set
	def numQueued(state: State): Long = state & TASK_MASK
	def numFutures(state: State): Long = (state & FUTURE_MASK) >> FUTURE_SHIFT
	def repr(state: State) = s"State(${numFutures(state)},${numQueued(state)})"
}

class SimpleExecutor[T](val limit: Int)(implicit ec: ExecutionContext) extends SequentialExecutor {
	import SimpleExecutor._
	import Log._

	private val queue = new ConcurrentLinkedQueue[EnqueueableTask]()

	// runState is:
	// xxxx|xxxx
	// ^ outstanding futures
	//       ^ queued tasks
	//
	// When we change from 0 to 1 queued task, we run. When shutting down, we always have at
	// least one slot to free, so the CAS ensures we are not confused about when we have
	// transitioned to the stopped state.
	//
	// When we get to `limit` outstanding futures, we also suspend. In that case each future's
	// onComplete will reschedule if (a) there is work queued, and (b) there were `limit` outstanding futures

	private val runState = new AtomicLong(0)

	// Note: doesn't need to be volatile, since we
	// only need to read writes that happened earlier in the same thread
	private var buffer = new Array[EnqueueableTask](limit)
	private val self = this
	private [capsul] def stateRepr = {
		val state = runState.get()
		(numFutures(state), numQueued(state))
	}

	override protected final def doEnqueue(task: EnqueueableTask): Boolean = {
		val logId = Log.scope(self, "doEnqueue")
		queue.add(task)
		val prevState = runState.getAndIncrement()
		val numQueued = SimpleExecutor.numQueued(prevState)
		val numFutures = SimpleExecutor.numFutures(prevState)
		log(s"enqueued task $task on top of ${repr(prevState)}")
		if ((numQueued + numFutures) < limit) {
			if (numQueued == 0) {
				ec.execute(workLoop)
			}
			true
		} else {
			false
		}
	}

	@tailrec private def acknowledgeAndBuffer(alreadyCompleted: Int, iterationsRemaining: Int): Int = {
		val logId = Log.scope(self, "WorkLoop.acknowledgeAndBuffer")
		val state = runState.get()

		// we can acknowledge up to limit tasks, minus outstanding futures
		val numFutures = SimpleExecutor.numFutures(state)
		val availableTasks = Math.min(limit - numFutures, numQueued(state) - alreadyCompleted)
		log(s"availableTasks = $availableTasks (from ${repr(state)}, with $alreadyCompleted already completed tasks")
		if (availableTasks == 0) {
			if (alreadyCompleted == 0) {
				// we're done, shut down
				log(s"shutting down ...")
				0
			} else {
				// nothing to do, but we've still got queued items to deduct
				if (runState.compareAndSet(state, state - alreadyCompleted)) {
					// no conflict, shut down
					log(s"shutting down after CASing state...")
					0
				} else {
					// conflict, retry
					acknowledgeAndBuffer(alreadyCompleted, iterationsRemaining)
				}
			}
		} else {

			if (iterationsRemaining > 0) {
				// there were availableTasks when we checked, but there's no harm in dequeueing extra
				// tasks if they're there by the time we grab them
				// val dequeueLimit = limit - numFutures
				// TODO: can we use the above code, instead of the more conservative number below?
				val dequeueLimit = availableTasks
				// acknowledge new tasks _before_ decrementing state, to ensure enqueuer won't acknowledge tasks later in the queue
				var acknowledgedTasks = 0
				var tries = 100
				while (acknowledgedTasks == 0) {
					tries -=1
					assert(tries >0)
					// loop until we've acknowledged at least one task
					log(s"dequeueing up to $dequeueLimit items into buffer (of $availableTasks available, state = ${repr(state)})")
					acknowledgedTasks = dequeueIntoBuffer(0, dequeueLimit)
					log(s"dequeued $acknowledgedTasks items into buffer")
				}
				if (alreadyCompleted != 0) {
					log(s"decrementing runState by $alreadyCompleted (to ${numQueued(state) - alreadyCompleted})")
					runState.addAndGet(-alreadyCompleted)
				}
				acknowledgedTasks
			} else {
				log("trampolining to prevent starvation")
				ec.execute(workLoop)
				0
			}
		}
	}

	@tailrec private def dequeueIntoBuffer(index: Int, maxDequeue: Long): Int = {
		// returns num items dequeued
		val logId = Log.scope(self, "WorkLoop.dequeueIntoBuffer")
		val item = queue.poll()
		log(s"dequeued item $item for buffer index $index (max=$maxDequeue)")
		if (item != null) {
			// TODO: can we figure out when to use enqueuedAsync instead of tryEnqueuedAsync?
			item.tryEnqueuedAsync()
			buffer.update(index, item)
			val nextIndex = index + 1
			if (nextIndex < maxDequeue) {
				dequeueIntoBuffer(nextIndex, maxDequeue)
			} else {
				nextIndex
			}
		} else {
			index
		}
	}

	// general flow:
	// runLoop(0) ->
	// - acknowledgeAndBuffer(alreadyCompleted = 0)
	//   - checks max available tasks min(numQueued, limit - numFutures)
	//   - dequeueIntoBuffer(0, maxAvailable)
	//     - return number of _actual_ items dequeued (<= maxAvailable)
	//   - ^ loop until nonzero
	//   - decrement by alreadyCompleted (if nonzero)
	//   - return number of dequeued tasks
	//   - recurce runLoop(numTasks)

	@tailrec private def runLoop(numCompleted: Int, iterationsRemaining: Int) {
		val logId = Log.scope(self, "WorkLoop.runLoop")

		val numTasks = acknowledgeAndBuffer(alreadyCompleted = numCompleted, iterationsRemaining = iterationsRemaining)
		log(s"After completing ${numCompleted}, there are ${numTasks} tasks acknowledged and ready to go (limit $limit)")
		if (numTasks > 0) {
			// perform all acknowledged tasks
			var taskIdx = 0
			while (taskIdx < numTasks) {
				// TODO: try to decrement state once fullyCompletedItems exceeds limit/2
				// (also acknowledge more tasks as we go)
				
				var task = buffer(taskIdx)
				task.run().filter(!_.isCompleted) match {
					case None => {
						log(s"ran sync node @ $taskIdx")
					}
					case Some(f) => {
						log(s"ran async node @ $taskIdx")
						self.runState.getAndAdd(SINGLE_FUTURE)
						f.onComplete { _ =>
							val prevState = self.runState.getAndAdd(-SINGLE_FUTURE)
							assert(numFutures(prevState) <= limit)
							log(s"one future completed in ${repr(prevState)}")
							if (numFutures(prevState) == limit && numQueued(prevState) > 0) {
								// we just freed up a slot to deque a pending task into. The loop
								// was stopped (waiting for futures), so we should run it again
								ec.execute(workLoop)
							}
						}
					}
				}
				buffer.update(taskIdx, null) // enable GC
				taskIdx += 1
			}
			// do another loop
			runLoop(numTasks, iterationsRemaining - numTasks)
		}
	}

	private [capsul] val workLoop: Runnable = new Runnable() {
		final def run(): Unit = {
			val logId = Log.scope(self, "WorkLoop")
			log("start")
			runLoop(0, 1000)
		}
	}
}
