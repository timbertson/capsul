package net.gfxmonk.sequentialstate

import java.util.concurrent.{Executors, ExecutorService}
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

object SequentialExecutorSpec {
	class Ctx(bufLen: Int, val ec: InspectableExecutionContext) {
		implicit val executionContext: ExecutionContext = ec
		var count = new Ctx.Count
		val ex = new SequentialExecutor(bufLen)

		def awaitAll[A](futures: List[Future[A]], seconds:Int = 10) =
			Await.result(Future.sequence(futures), seconds.seconds)

		def queuedRunLoops = {
			ec.queue.filter(_ == ex.workLoop)
		}

		def inc(sleep: Int = 10): UnitOfWork.Full[Int] = {
			UnitOfWork.Full(() => {
				assert(count.busy == false)
				val initial = count.current
				count.busy = true
				Thread.sleep(sleep)
				assert(count.busy == true)
				count.current = initial + 1
				count.busy = false
				count.current
			})
		}
	}

	object Ctx {
		private var threadPool:Option[ExecutorService] = None

		def withManualExecution(bufLen: Int) = new Ctx(bufLen, manualExecutionContext)
		def withThreadPool(bufLen: Int) = new Ctx(bufLen, threadpoolExecutionContext)

		def manualExecutionContext = new ManualExecutionContext
		def threadpoolExecutionContext = {
			new CountingExecutionContext(ExecutionContext.fromExecutor(threadPool.get))
		}

		def getThreadPool = threadPool.getOrElse(
			throw new RuntimeException("Ctx.init() not called")
		)

		def init() = {
			if (threadPool.isDefined) {
				throw new RuntimeException("Ctx.init() called twice")
			}
			threadPool = Some(Executors.newFixedThreadPool(3))
		}
		def shutdown() = threadPool.foreach(_.shutdown())

		class Count {
			var current = 0
			var busy = false
			override def toString() = "Count("+current+","+busy+")"
		}
	}
}

class SequentialExecutorSpec extends FunSpec with BeforeAndAfterAll {
	import SequentialExecutorSpec._
	override def beforeAll = Ctx.init()
	override def afterAll = Ctx.shutdown()

	it("delays job enqueue once capacity is reached") {
		val ctx = Ctx.withManualExecution(3); import ctx._
		val futures = List.fill(4)(ex.enqueueRaw(inc()))
		assert(futures.map(_.isCompleted) == List(true, true, true, false))
	}

	it("enqueues waiting jobs upon task completion") {
		val ctx = Ctx.withManualExecution(3); import ctx._
		val futures = List.fill(4)(ex.enqueueRaw(inc()))

		assert(futures.map(_.isCompleted) == List(true, true, true, false))

		assert(ec.queue.length == 1)
		ec.queue(0).run()

		// extra enqueue was submitted
//		assert(ec.queue.length == 2)
//		ec.queue(1).run()

		assert(futures.map(_.isCompleted) == List(true, true, true, true))
	}

	it("runs queued jobs in a single execution") {
		val ctx = Ctx.withManualExecution(3); import ctx._
		val futures = List.fill(3)(ex.enqueueRaw(inc()))
		assert(futures.map(_.isCompleted) == List(true, true, true))

		ec.queue.head.run()

		assert(futures.map(_.value.get.get.isCompleted) == List(true, true, true))
		assert(futures.map(_.value.get.get.value.get.get) == List(1, 2, 3))
		assert(ec.queue.length == 1)
	}

	it("executes all jobs in sequence") {
		val ctx = Ctx.withThreadPool(3); import ctx._

		// big sleep ensures that if we're not running in sequence,
		// we'll encounter race conditions due to `inc()` not being thread-safe
		awaitAll(List.fill(4)(ex.enqueueReturn(inc(sleep=50))))

		assert(queuedRunLoops.length == 1)
		assert(count.current == 4)
	}

	it("executes up to 200 jobs in a single loop") {
		val ctx = Ctx.withThreadPool(bufLen = 400); import ctx._

		awaitAll(List.fill(200)(ex.enqueueReturn(inc(sleep=1))))
		assert(queuedRunLoops.length == 1)
		assert(count.current == 200)
	}

	it("defers jobs into a new loop after 200 to prevent starvation") {
		val ctx = Ctx.withThreadPool(bufLen = 400); import ctx._

		awaitAll(List.fill(201)(ex.enqueueReturn(inc(sleep=1))))
		assert(queuedRunLoops.length == 2)
		assert(count.current == 201)
	}
}
