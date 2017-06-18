import scala.io._
import org.scalatest._
import monix.execution.Scheduler.Implicits.global
import monix.eval._
import monix.reactive._
import monix.reactive.OverflowStrategy.BackPressure
import scala.concurrent.duration._
import scala.concurrent._
import scala.collection.mutable._
import scala.util.Random

class MonixSpec extends FunSpec {
	import MapAsyncOrdered._
	def nextTick() = Thread.sleep(100L)

	it("tests") {
		val numbers = Stream.from(1).take(10)
		val obs = Observable.fromIterable(numbers)
		val inProcess = Map[Int,Promise[Unit]]()
		val results = ListBuffer[Int]()
		var error: Option[Throwable] = None
		val outputComplete =
			obs.parallelMapPreservingOrder(5)(i => {
				Task.fromFuture(Future {
					val promise = Promise[Unit]()
					println("created: " + i)
					inProcess += i -> promise
					promise.future.map(_ => {
						println("resolved: " + i)
						i
					})
				}.flatMap(identity))
			})
			.doOnError( err => {
				error = Some(err)
			})
			.foreach (i => {
				inProcess -= i
				println("got: " + i)
				results.append(i)
			})

		// var line = ""
		// do {
		// 	line = Console.readLine()
		// 	if (line != "") {
		// 		val i = line.toInt
		// 		println("\ncompleting: " + i)
		// 		inProcess(i).success()
		// 	}
		// } while(line != "")

		// output is now running, outputComplete should never terminate
		assert(outputComplete.value == None)
		nextTick()

		// first, we fill the buffer
		assert(inProcess.keys.toList.sorted == List(1,2,3,4,5))
		assert(results.toList == List())

		// completing out of order doesn't emit anything
		inProcess(2).success(())
		inProcess(3).success(())
		nextTick()

		assert(results.toList == List())

		// but completing the first will drain all complete, in order
		inProcess(1).success(())
		nextTick()

		assert(results.toList == List(1,2,3))
		assert(inProcess.keys.toList.sorted == List(4,5,6,7,8))

		inProcess(4).success()
		nextTick()
		assert(inProcess.keys.toList.sorted == List(5,6,7,8,9))

		val err = new RuntimeException("testing")
		inProcess(5).failure(err)
		nextTick()
		assert(error == Some(err))
	}
}
