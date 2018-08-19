package net.gfxmonk.capsul.mini

import java.util.concurrent.atomic._
import java.util.concurrent.{Executors, TimeUnit, ForkJoinPool}
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.annotation.tailrec

import org.openjdk.jcstress.annotations.Expect._
import org.openjdk.jcstress.annotations._
import org.openjdk.jcstress.infra.results._

import net.gfxmonk.capsul.testsupport

@JCStressTest
@Outcome(
	id = Array(
		// options:
		"1002/1", // all tasks are picked up in a single loop, if queued is 0 when the 1000th item is completed
		"1002/2", // one secondary loop occurs, either due to race or trampoline
		"1002/3",
		"1002/4"
	), expect = ACCEPTABLE,
	desc = "enqueue a pair of items past the 1000 limit")
@State
class MiniStressTest {
	import testsupport.Common._
	val numItems = 1000
	implicit val ec = new CountingExecutionContext(defaultEc)
	val executor = SequentialExecutor.unbounded()
	var counter = 0
	def work = new Ask(() => {
		Thread.sleep(0)
		counter += 1
		()
	})

	repeatList(numItems - 4) {
		executor.enqueue(work)
	}
	val secondLastItem = executor.enqueue(work)
	executor.enqueue(work)

	@volatile var finalA: Future[Unit] = null
	@volatile var finalB: Future[Unit] = null
	ec.resetCount()

	@Actor
	def actor1() {
		executor.enqueue(work)
		await(secondLastItem)
		finalA = executor.enqueue(work)
	}

	@Actor
	def actor2() {
		executor.enqueue(work)
		await(secondLastItem)
		finalB = executor.enqueue(work)
	}

	@Arbiter
	def arbiter(r: L_Result) {
		List(finalA, finalB).foreach(await)
		r.r1 = s"${counter}/${ec.numTasks}"
	}
}
