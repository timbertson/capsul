import java.nio.charset.Charset

import ThreadState.EnqueueResult
import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Success, Try}


class SampleActorWordCount(implicit sched: ExecutionContext) {
	val state = SequentialState(0)
	def feed(line: String) = state.mutate { state =>
		val old = state.get
		state.set(state.get + line.split("\\w").length)
		Thread.sleep(1000)
		if (old > 12380) {
			// XXX hacky
//			println(s"updated word count from: $old -> ${state.get}")
		}
	}

	def reset() = state.mutate(_.set(0))
	def get() = state.read(identity)
	def print() = state.read(println)
}

class SampleActorLineCount(implicit ec: ExecutionContext) {
	val state = SequentialState(0)
	def inc() = state.mutate { state => state.set(state.get + 1) }
	def get() = state.read(identity)
}

object ActorExample {
	import scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def countWithSequentialStates(lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleActorWordCount()
		val lineCount = new SampleActorLineCount()


		def loop(): Future[(Int, Int)] = {
			println("loop")
			if (lines.hasNext) {
				for {
					numWords: Future[Unit] <- wordCounter.feed(lines.next())
					numLines: Future[Unit] <- lineCount.inc()
					// XXX potentially no need to wait until the last round?
//					result <- numWords.zip(numLines).flatMap(_ => loop())
						result <- (if (lines.hasNext) loop() else numWords.zip(numLines).flatMap(_ => loop()))
				} yield result
			} else {
//				println("final line!")
				for {
					words <- wordCounter.get()
					lines <- lineCount.get()
					result <- words.zip(lines)
				} yield result
			}
		}

		loop()
	}

	def time(name:String, impl: => Future[(Int,Int)]) = {
		val start = System.currentTimeMillis()
//		println("Start")
		val f = impl
//		println("(computed)")
		val counts = Await.result(f, Duration.Inf)
		val end = System.currentTimeMillis()
		val duration = end - start
//		println("Done")
		println(s"implementation $name took $duration ms to calculate $counts")
	}

	def run(): Unit = {
		val numLines = 30
		var attempt = 1
		while(attempt > 0) {
			time("SequentialState", countWithSequentialStates(makeLines(numLines)))
			attempt -= 1
		}
	}
}


object ActTest {
	def main() {
		ActorExample.run
	}
}
