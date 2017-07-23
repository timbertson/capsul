package net.gfxmonk.sequentialstate
import java.util.concurrent.{Executors, ExecutorService}
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

trait InspectableExecutionContext extends ExecutionContext {
	def queue: Queue[Runnable]
}

class ManualExecutionContext extends InspectableExecutionContext {
	var queue = Queue.empty[Runnable]

	override def execute(runnable: Runnable) = {
		queue = queue.enqueue(runnable)
	}

	override def reportFailure(cause: Throwable) = throw cause
}

class CountingExecutionContext(ec: ExecutionContext) extends InspectableExecutionContext {
	def count = queue.length
	var queue = Queue.empty[Runnable]

	override def execute(runnable: Runnable) = {
		queue = queue.enqueue(runnable)
		ec.execute(runnable)
	}

	override def reportFailure(cause: Throwable) = throw cause
}

