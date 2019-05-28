package net.gfxmonk.capsul.examples.readme

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import net.gfxmonk.capsul._

object Main {
	def run(): Future[String] = {
		// create a Capsul[Int]
		val state = Capsul(0)

		def addOne() = state.sendTransform(_ + 1)

		def addTwoInParallel() = Future.sequence(List(
			Future(addOne()),
			Future(addOne())
		)).map(_ => ())

		for {
			// kick off two parallel actions which
			// both add one to the state. Despite accessing
			// `state` from different threads, there are no
			// data races since actions occur sequentially
			() <- addTwoInParallel()

			// then double it (fire-and-forget)
			() <- state.sendTransform(_ * 2)

			// mutate the state and also return a result
			// (waits for the result, not fire-and-forget)
			formatted <- state.mutate { ref =>
				ref.set(ref.current + 3)
				s"The counter ended up being: ${ref.current}"
			}.flatten
		} yield formatted
	}

	def main() {
		println(Await.result(run(), Duration.Inf))
	}
}
