package net.gfxmonk.capsul

import java.util.concurrent.atomic._
import java.util.concurrent.{Executors, TimeUnit, ForkJoinPool}
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.annotation.tailrec

import org.openjdk.jcstress.annotations.Expect._
import org.openjdk.jcstress.annotations._
import org.openjdk.jcstress.infra.results._

@JCStressTest
@Outcome(
	id = Array(
		"(head=3, tail=3, queued=0, futures=1)" // 1 incomplete future, 3 items succeeded
	), expect = ACCEPTABLE,
	desc = "enqueue excess sync item while completing an async item")
@State
class QueueStressTest {
	import testsupport.Common._
	implicit val ec = new CountingExecutionContext(defaultEc)
	val executor = new SimpleExecutor(bufLen)
	def futureWork(f: Future[Unit]) = UnitOfWork.FullAsync(() => f)
	def noopWork = UnitOfWork.Full(() => ())
	def noopEnqueue = UnitOfWork.EnqueueOnly(() => ())
	val readyFutures = repeatList(bufLen - 1) {
		val promise = Promise[Unit]()
		executor.enqueue(futureWork(promise.future))
		promise
	}
	var syncFuture: Future[Unit] = null
	ec.resetCount()

	@Actor
	def actor1() {
		// complete a future
		readyFutures.head.success(())
	}

	@Actor
	def actor2() {
		// enqueue a sync item
		syncFuture = executor.enqueue(noopWork)
	}

	@Arbiter
	def arbiter(r: L_Result) {
		// await(syncFuture)
		val incompletePromises = readyFutures.tail
		// println(readyFutures)

		@tailrec
		def check(attempts: Int): String = {
			val (numFutures, queued) = executor.stateRepr
			val desc = s"(numFutures=$numFutures, queued=$queued)"

			// println(s"attempt[$attempts], ${desc}")

			// we want to end up in a state where:
			//  - there's `incompletePromises.count` in `numFutures`
			//  - it's not running
			//  - head == tail (this should be covered by a stopped state, since we access state before head)
			if (
				numFutures == incompletePromises.length &&
				queued == 0)
			{
				// println(s"got it! isStopped = ${Ring.isStopped(state)} && ${numFutures} == ${incompletePromises.length} && ${Ring.tailIndex(state)} == $head  ;;;;  $desc")
				desc
			} else {
				if(attempts == 0) {
					// println("Gave up :(")
					desc
				} else {
					Thread.sleep(1)
					check(attempts - 1)
				}
			}
		}
		r.r1 = check(40)

		// cleanup
		incompletePromises.foreach(_.success(()))
		await(executor.enqueue(noopWork))
	}
}
