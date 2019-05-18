package net.gfxmonk.capsul.mini

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class BackpressureSpec extends FunSpec with TimeLimitedTests with Matchers with PropertyChecks {
	val timeLimit = 5.seconds

	it("represents states correctly") {
		forAll { (busy: Int, queued: Int) =>
			whenever(busy >=0 && queued >=0) {
				assert(State.repr(State.make(busy, queued)) == (busy, queued))
			}
		}
	}

	it("limits running tasks to $capacity") {
		val capacity = 4
		implicit val _ = Scheduler.fixedPool("test", capacity*2)
		def pause() { Thread.sleep(100) }
		val nextIndex = AtomicInt(0)
		val waiting = AtomicInt(0)
		val waiters = Range(0, capacity*3).map(_ => new Object()).toList
		def synchronized(fn: Object => Unit)(obj: Object) {
			obj.synchronized(fn(obj))
		}
		val wait = synchronized(_.wait()) _
		val notify = synchronized(_.notify()) _

		val task = Task {
			val idx = nextIndex.getAndIncrement()
			waiting.increment()
			try {
				wait(waiters(idx))
			} catch {
				case e:Throwable => println(e)
			} finally {
				waiting.decrement()
			}
			idx
		}

		val bp = TaskBuffer(capacity)
		val futures = Range(0, capacity+1).map(_ => bp.run(task)).toList
		def futuresCompleted =
			futures.map(_.value.map(_.get.isCompleted))
		def futureValues =
			futures.map(_.value.flatMap(_.get.value.map(_.get)))

		// give a moment to ensure we're not going to get any further waiters
		pause()
		waiting.get.shouldEqual(capacity)
//		println(futureValues)
		assert(nextIndex.get == capacity)
		assert(futuresCompleted == List(Some(false), Some(false), Some(false), Some(false), None))

		// trigger one completion, which will run the next waiter
		notify(waiters.head)
		pause()
//		println(futuresCompleted)
//		println(futureValues)
		assert(nextIndex.get == capacity+1)
		waiting.get.shouldEqual(capacity)
		assert(futuresCompleted ==
			List(Some(true), Some(false), Some(false), Some(false), Some(false)))

		waiters.foreach(notify)
		pause()
//		println(futureValues)
		assert(futureValues.sorted ==
			Range(0, capacity+1).toList.map(Some.apply))
	}
}
