package net.gfxmonk.sequentialstate.example.async
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

import akka.actor.{ActorSystem,Actor}
import akka.stream.{Materializer,ActorMaterializer}
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global

// There are better caches out there, but this example hopefully shows
// how you might use SequentialState to manage access to some async
// resource with your own bookkeeping.

object Api {
	def get(resource: String) = {
		// shh, just pretend this is calling a server somewhere
		Future {
			Thread.sleep(1000)
			s"Tada! Here's that $resource you asked for.."
		}
	}
}

object StateBased {
	class Cache() {
		// Provides a cache in front of API requests, 5 of which
		// actual requests are allowed at any point.
		private val state = SequentialState(new mutable.Map())
		private val apiSemaphore = TaskSemaphore(5)
		def get(resource: String): Future[Future[String]] = state.awaitAccess(cache => {
			cache.get(resource) match {
				case Some(cached) => cached
				case None => {
					// Break up response into an outer future which resolves once the request has been
					// accepted, and an inner future (the result) in order to maintain backpressure
					val future = apiSemaphore.acquire.runAsync.map(_ => {
						val result = Api.get(resource)
						result.onComplete(apiSemaphore.release.runAsync)
						result
					})
					cache.update(resource, future)
					future
				}
			}
		}).flatMap(identity)
	}

	def downloadAll(resources: Iterable[String]): Future[List[Resources]] = {
		val cache = new Cache()

		val initial = Queue.empty[Future[String]]

		// fetches builds up a list of accepted requests - these may be ready now, or
		// they may be in-flight.
		val fetches = lines.foldLeft(Future.successful(initial)) { (fetching, resource) =>
			for {
				accum <- fetching
				fetch <- cache.get(resource)
			} yield accum.enqueue(fetch)
		}

		fetches.flatMap(Future.sequence).map(_.toList)
	}
}

object ActorBased {

	class Cache extends Actor {
		import Cache._
		private val apiSemaphore = TaskSemaphore(5)
		private val cache = new mutable.Map()
		def receive = {
			// note: doesn't allow for any queueing above what `apiSemaphore` allows,
			// so cached results are effectly served synchronously
			case Request(resource:String) => cache.get(resource) match {
				case Some(cached) => cached
				case None => {
					val future = apiSemaphore.acquire.runAsync.map(_ => {
						val result = Api.get(resource)
						result.onComplete(apiSemaphore.release.runAsync)
						result
					})
					cache.update(resource, future)
					future
				}
			}
		}
	}

	object Cache {
		case class Request(resource: String)
	}

	def downloadAll(resources: Iterable[String])(implicit system: ActorSystem): Future[List[Resources]] = {
		val cache = new Cache()

		val initial = Queue.empty[Future[String]]

		val fetches = lines.foldLeft(Future.successful(initial)) { (fetching, resource) =>
			for {
				accum <- fetching
				response <- (cache ? Cache.Request(resource)).mapTo[Future[Future[String]]]
				fetch <- response
			} yield accum.enqueue(fetch)
		}

		fetches.flatMap(Future.sequence).map(_.toList)
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
