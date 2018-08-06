package net.gfxmonk.capsul
import java.util.concurrent.{Executors, ExecutorService}
import org.scalatest._
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

trait InspectableExecutionContext extends ExecutionContext {
	def queue: Queue[Runnable]
	def runOne(): Unit
	def runUntilEmpty(): Unit
}

class ManualExecutionContext extends InspectableExecutionContext {
	var queue = Queue.empty[Runnable]

	override def runOne(): Unit = {
		queue.head.run()
		queue = queue.tail
	}

	override def execute(runnable: Runnable) = {
		queue = queue.enqueue(runnable)
	}

	override def reportFailure(cause: Throwable) = throw cause

	override def runUntilEmpty(): Unit = {
		val tasks = queue
		queue = Queue.empty
		tasks.foreach(_.run())
		if (queue.nonEmpty) runUntilEmpty()
	}
}

class CountingExecutionContext(ec: ExecutionContext) extends InspectableExecutionContext {
	def count = queue.length
	var queue = Queue.empty[Runnable]

	override def runOne(): Unit = ???
	override def runUntilEmpty(): Unit = ???

	override def execute(runnable: Runnable) = {
		queue = queue.enqueue(runnable)
		ec.execute(runnable)
	}

	override def reportFailure(cause: Throwable) = throw cause
}

