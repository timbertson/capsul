package net.gfxmonk.sequentialstate.example.async
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.TaskSemaphore
import monix.execution.Scheduler.Implicits.global
import net.gfxmonk.sequentalstate.sample.FutureUtils
import net.gfxmonk.sequentialstate._

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

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
		private val state = SequentialState(mutable.Map[String,Future[Future[String]]]())
		private val apiSemaphore = TaskSemaphore(5)
		def get(resource: String): Future[Future[String]] = state.awaitAccess(cache => {
			cache.get(resource) match {
				case Some(cached) => cached
				case None => {
					// Break up response into an outer future which resolves once the request has been
					// accepted, and an inner future (the result) in order to maintain backpressure
					val future = apiSemaphore.acquire.runAsync.map(_ => {
						val result = Api.get(resource)
						result.onComplete(_ => apiSemaphore.release.runAsync)
						result
					})
					cache.update(resource, future)
					future
				}
			}
		}).flatMap(identity)
	}

	def downloadAll(resources: Iterable[String]): Future[List[String]] = {
		val cache = new Cache()

		// `fetches` builds up a list of accepted requests - these may be
		// ready now, or they may be in-flight.
		val fetches: Future[Queue[Future[String]]] =
			FutureUtils.foldLeft(Queue.empty[Future[String]], resources) {
				(accum, resource) => cache.get(resource).map(x => accum.enqueue(x))
			}

		fetches.flatMap(fs => Future.sequence(fs).map(_.toList))
	}
}

object ActorBased {

	class Cache extends Actor {
		import Cache._
		private val apiSemaphore = TaskSemaphore(5)
		private val cache = mutable.Map[String,Future[Future[String]]]()
		def receive = {
			// note: doesn't allow for any queueing above what `apiSemaphore` allows,
			// so cached results are effectly served synchronously
			case Request(resource:String) => cache.get(resource) match {
				case Some(cached) => sender ! cached
				case None => {
					val future = apiSemaphore.acquire.runAsync.map(_ => {
						val result = Api.get(resource)
						result.onComplete(_ => apiSemaphore.release.runAsync)
						result
					})
					cache.update(resource, future)
					sender ! future
				}
			}
		}
	}

	object Cache {
		case class Request(resource: String)
	}

	def downloadAll(resources: Iterable[String])
		(implicit system: ActorSystem): Future[List[String]] =
	{
		val cache = system.actorOf(Props[Cache])

		implicit val duration: Timeout = 60 seconds
		val fetches: Future[Queue[Future[String]]] = FutureUtils.foldLeft(Queue.empty[Future[String]], resources)(
			(accum, resource) => {
				for {
					response <- (cache ? Cache.Request(resource)).mapTo[Future[Future[String]]]
					fetch <- response
				} yield accum.enqueue(fetch)
			}
		)

		fetches.flatMap((fs) => Future.sequence(fs).map(_.toList))
	}
}


object ExampleMain {
	def main() {
		val system = ActorSystem("akka-example")
		val resources = List("/id/1", "/id/2", "/id/3", "/id/4", "/id/5")
		println("Actor-based: " + Await.result(ActorBased.downloadAll(resources)(system), Duration.Inf))
		println("State-based: " + Await.result(StateBased.downloadAll(resources), Duration.Inf))
		Await.result(system.terminate(), Duration.Inf)
	}
}
