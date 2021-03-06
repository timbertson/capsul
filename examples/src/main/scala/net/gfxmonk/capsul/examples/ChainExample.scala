package net.gfxmonk.capsul.examples.chain
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import monix.eval.TaskSemaphore
import monix.execution.Scheduler.Implicits.global
import net.gfxmonk.capsul.examples.FutureUtils
import net.gfxmonk.capsul._
import net.gfxmonk.capsul.staged._

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._

// Example of a chaining together multiple sequential states, maintaining backpressure

object StateBased {

	class Prefixer(next: Option[Prefixer]) {
		private val state = Capsul("")
		def initPrefix(prefix: String): Future[Unit] = state.sendSet(prefix)

		def prefix(value: String): StagedFuture[String] = {
			// accessStaged will delay acceptance until the outer
			// future is complete. Which is the `enqueue` of the following actor.
			//
			// The result is that if the final actor in the chain is at capacity,
			// intermediate buffers will fill but not overflow, since they
			// don't pop an item until it's accepted downstream
			state.accessStaged(prefix => {
				val prefixed = prefix + value
				next match {
					case Some(prefixer) => prefixer.prefix(prefixed)
					case None => StagedFuture.successful(prefixed)
				}
			})
		}

		def prefixAsync(value: String): StagedFuture[String] = {
			state.accessStaged(prefix => {
				val prefixed = Future {
					//inexplicably async, for demonstration purposes
					Thread.sleep(1000)
					prefix + value
				}

				next match {
					case Some(prefixer) => prefixed.flatMapStaged(prefixed => prefixer.prefixAsync(prefixed))
					case None => StagedFuture.accepted(prefixed)
				}
			})
		}
	}

	def applyMultiplePrefixes(value: String): Future[String] = {
		val a = new Prefixer(None)
		val b = new Prefixer(Some(a))
		val c = new Prefixer(Some(b))

		for {
			() <- a.initPrefix("a ")
			() <- b.initPrefix("then b ")
			() <- c.initPrefix("then c:")
			result <- c.prefix(value)
		} yield result
	}

	def applyMultiplePrefixesAsync(value: String): Future[String] = {
		val a = new Prefixer(None)
		val b = new Prefixer(Some(a))
		val c = new Prefixer(Some(b))

		for {
			() <- a.initPrefix("a ")
			() <- b.initPrefix("then b ")
			() <- c.initPrefix("then c:")
			result <- c.prefixAsync(value)
		} yield result
	}
}

object ExampleMain {
	def main() {
		println("Multi-prefix: " + Await.result(StateBased.applyMultiplePrefixes("value"), Duration.Inf))
		println("Multi-prefix (async): " + Await.result(StateBased.applyMultiplePrefixesAsync("value"), Duration.Inf))
	}
}
