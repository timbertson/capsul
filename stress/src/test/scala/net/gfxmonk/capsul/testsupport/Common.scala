package net.gfxmonk.capsul.testsupport

import java.util.concurrent.atomic._
import java.util.concurrent.{Executors, TimeUnit, ForkJoinPool}
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import org.openjdk.jcstress.annotations.Expect._
import org.openjdk.jcstress.annotations._
import org.openjdk.jcstress.infra.results._

object Common {
	val bufLen = 3
	val parallelism = 3
	def await[T](f: Future[T]) = Await.result(f, Duration(2, TimeUnit.SECONDS))
	def repeat[T](times: Int)(fn: => T): T = {
		var remaining = times
		while(remaining > 1) {
			fn
			remaining -= 1
		}
		fn
	}
	def repeatList[T](times: Int)(fn: => T): List[T] = {
		var remaining = times
		var ret = List[T]()
		while(remaining > 0) {
			ret = fn :: ret
			remaining -= 1
		}
		ret.reverse
	}

	def makeEc(size: Int = parallelism): ExecutionContext = {
		val threadPool = new ForkJoinPool(parallelism)
		ExecutionContext.fromExecutor(threadPool)
	}
	val defaultEc = makeEc(parallelism)

	class CountingExecutionContext(c: ExecutionContext) extends ExecutionContext {
		private val tasks = new AtomicInteger(0)

		override def execute(runnable: Runnable) {
			tasks.incrementAndGet()
			c.execute(runnable)
		}

		override def reportFailure(cause: Throwable) {
			c.reportFailure(cause)
		}

		def numTasks = tasks.get
		def resetCount() = tasks.set(0)
	}
}
