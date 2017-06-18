import monix.execution.Scheduler.Implicits.global
import monix.eval._
import monix.reactive._
import monix.reactive.OverflowStrategy.BackPressure
import scala.concurrent.duration._
import scala.concurrent._
import scala.collection.mutable._
import scala.util.Random
import MapAsyncOrdered._

object MonixTest extends App {

	println("go...")
	val rand = new Random()
	// val items = 0 until 10
	val items = scala.collection.mutable.Queue(
		100, 500, 100, 200, 300, 500, 200,
		100, 500, 100, 200, 300, 500, 200,
		100, 500, 100, 200, 300, 500, 200,
		100, 500, 100, 200, 300, 500, 200,
		100, 500, 100, 200, 300, 500, 200
		)
	var inflight = 0
	val source = Observable.fromIterator(
			Iterator.continually {
				// Thread.sleep(100)
				try {
					val item = items.dequeue()
					println("producing item: " + item)
					Some(item)
				} catch {
					case e:java.util.NoSuchElementException => None
				}
			}.takeWhile(_.isDefined).map(_.get)
		)
			.parallelMapPreservingOrder(4)(i => {
			Task({
				inflight+=1
				println("Starting, inflight = " + inflight)
				println(s"task for $i, run on thread ${Thread.currentThread.getName}")
				// Thread.sleep(i)
				i
			})
			.delayResult(i.milliseconds)
			// .runAsync
		})
		// .asyncBoundary(BackPressure(7))
		// .mapFuture(x => {
		// 	x.map { x =>
		// 		inflight-=1
		// 		println("resolved: " + x + ", inflight = " + inflight)
		// 		x
		// 	}
		// })

		val task = source.foreach { x =>
			println("Got: " + x)
		}

		try {
			Await.result(task, 1.seconds)
		} catch {
			case (e:java.util.concurrent.TimeoutException) => println("timed out ...")
		} finally {
			task.cancel()
		}

	// // The list of all tasks needed for execution
	// val tasks = items.map(i => {
	// 	println(s"task for $i, created on thread ${Thread.currentThread.getName}")
	// 	Task({
	// 		println(s"task for $i, run on thread ${Thread.currentThread.getName}")
	// 		i*2
	// 	}).delayResult(i.seconds).runAsync
	// })
	// .whileBusyBuffer(BackPressure(3))
	// .mapFuture(identity)
  //
	// // Processing in parallel
	// // val aggregate = Task.gather(tasks).map(_.toList)
  //
	// // Evaluation:
	// val result = Await.result(tasks.foreach(println), 20.seconds)
	// println(s"done: $result")
	//=> List(0, 2, 4, 6, 8, 10, 12, 14, 16,...
}
