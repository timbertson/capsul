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
		if (old > 12380) {
			// XXX hacky
//			println(s"updated word count from: $old -> ${state.get}")
		}
	}

	def reset() = state.mutate(_.set(0))
	def get() = state.read(identity)
	def print() = state.read(println)
}

class SampleCountingActor(bufLen: Int)(implicit ec: ExecutionContext) {
	val state = SequentialState(bufLen = bufLen, v = 0)
	def inc() = state.mutate { state =>
		state.set(state.get + 1)
	}
	def get() = state.read(identity)
}

object ActorExample {
	import scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def simpleCounter(bufLen: Int, limit: Int): Future[Int] = {
		val lineCount = new SampleCountingActor(bufLen=bufLen)
		def loop(i: Int): Future[Int] = {
			lineCount.inc().flatMap { complete: Future[Unit] =>
				val nextI = i+1
				if (nextI == limit) {
					complete.flatMap { (_:Unit) =>
						lineCount.get().flatMap(identity)
					}
				} else {
					loop(nextI)
				}
			}
		}
		loop(0)
	}

	def countWithSequentialStates(bufLen: Int, lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleActorWordCount()
		val lineCount = new SampleCountingActor(bufLen = bufLen)
		def loop(): Future[(Int, Int)] = {
			if (lines.hasNext) {
				for {
					numWords: Future[Unit] <- wordCounter.feed(lines.next())
					numLines: Future[Unit] <- lineCount.inc()
					// XXX potentially no need to wait until the last round?
					// result <- numWords.zip(numLines).flatMap(_ => loop())
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

	def time(name:String, impl: => Future[_]) = {
		val start = System.currentTimeMillis()
//		println("Start")
		val f = impl
//		println("(computed)")
		val result = Await.result(f, Duration.Inf)
		val end = System.currentTimeMillis()
		val duration = end - start
//		println("Done")
		println(s"implementation $name took $duration ms to calculate $result")
	}

	def repeat(n: Int)(f: => Unit) {
		var attempt = n
		while(attempt>0) {
			f
			attempt -= 1
		}
	}

	def run(): Unit = {
		val repeat = this.repeat(20) _
		val bufLen = 4

		// count lines
		repeat {
			val numLines = 3000
			time("SequentialState: word count", countWithSequentialStates(bufLen = bufLen, lines=makeLines(numLines)))
		}

		repeat {
			// simple counter
			val simpleCountLimit = 10000
			time("SequentialState: simple counter", simpleCounter(bufLen = bufLen, limit=simpleCountLimit))
		}
	}
}


object ActTest {
	def main() {
		ActorExample.run
	}
}
