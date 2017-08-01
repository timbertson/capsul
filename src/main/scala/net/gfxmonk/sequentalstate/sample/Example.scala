package net.gfxmonk.sequentialstate.example.wordcount
import net.gfxmonk.sequentialstate._
import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._
import akka.actor.{Actor, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import net.gfxmonk.sequentalstate.sample.FutureUtils

import scala.concurrent.ExecutionContext.Implicits.global

object StateBased {
	class WordCounter() {
		private val state = SequentialState(0)
		def add(line: String) = state.sendTransform(_ + line.split("\\w").length)
		def get = state.current
	}

	class Counter() {
		val state = SequentialState(v = 0)
		def inc() = state.sendTransform(_ + 1)
		def get = state.current
	}

	def runWihoutBackpressure(lines: Iterable[String]) = {
		val wordCounter = new WordCounter()
		val counter = new Counter()

		lines.foreach { line =>
			wordCounter.add(line)
			counter.inc()
		}

		for {
			words <- wordCounter.get
			lines <- counter.get
		} yield (words, lines)
	}

	def run(lines: Iterable[String]) = {
		val wordCounter = new WordCounter()
		val counter = new Counter()

		// send off all our counting actions
		val counted = FutureUtils.foldLeft((), lines) ((_:Unit, line) => {
			for {
				() <- wordCounter.add(line)
				() <- counter.inc()
			} yield ()
		})

		// once all actions have been sent, request the final numbers
		for {
			() <- counted
			words <- wordCounter.get
			lines <- counter.get
		} yield (words, lines)
	}
}

object ActorBased {

	class Counter extends Actor {
		import Counter._
		private var i = 0
		def receive = {
			case Increment => i += 1
			case Get => sender ! i
		}
	}

	object Counter {
		case object Increment
		case object Get
	}

	class WordCounter extends Actor {
		import WordCounter._
		private var i = 0
		def receive = {
			case Add(line) => i += line.split("\\w").length
			case Get => sender ! i
		}
	}

	object WordCounter {
		case object Get
		case class Add(line: String)
	}

	def run(lines: Iterable[String])(implicit system: ActorSystem) = {
		val wordCounter = system.actorOf(Props[WordCounter])
		val counter = system.actorOf(Props[Counter])
		implicit val duration: Timeout = 5 seconds

		lines.foreach { line =>
			wordCounter ! WordCounter.Add(line)
			counter ! Counter.Increment
		}

		for {
			words <- (wordCounter ? WordCounter.Get).mapTo[Int]
			lines <- (counter ? Counter.Get).mapTo[Int]
		} yield (words, lines)
	}
}


object ExampleMain {
	def main() {
		val system = ActorSystem("akka-example")
		val lines = List("fdsf  fdfkdsh fhsdjk", "fdj fjfgh dfkgh fd", "gfdfhgjkdf hgfgdf df gdf gfd gdfg df")
		println("Actor-based: " + Await.result(ActorBased.run(lines)(system), Duration.Inf))
		println("State-based: " + Await.result(StateBased.run(lines), Duration.Inf))
		Await.result(system.terminate(), Duration.Inf)
	}
}
