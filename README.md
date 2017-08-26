# SequentialState

A minimal, type-safe alternative to (some of) akka.

## Why should I use SequentialState?

Parallel programming (using locks, mutexes, etc) is still a surprisingly error-prone task even in modern scala. While you rarely need to implement your own concurrency primitives, you do need to use the builtin ones correctly. That isn't always straightforward or efficient, and frequently requires diligence that cannot be enforced by the compiler.

I liked the simple concurrency guarantees of actors, but found that akka was unfitting for many purposes. I realised that I didn't want my system to be composed of actors, I was happy with _mostly_ stateless functional programming. But for the places where I must maintain state, I needed something better than a lock.

Also, when learning about akka-streams and monix (and the reactive streams protocol), I was dismayed to realise that plain akka provides no building blocks for dealing with backpressure and preventing overload.

SequentialState provides the same concurrency model as local actors, plus type safety and builtin support for non-blocking backpressure. It achieves this by simply encapsulating state, doing away with most other features of actors.


## Simple example:

```scala
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import net.gfxmonk.sequentialstate._

object Main {
	def run(): Future[String] = {
		// create a SequentialState[Int]
		val state = SequentialState(0)

		def addOne() = state.sendTransform(_ + 1)

		// This is a bit unnecessary, just showing that
		// state can be safely accessed from two different
		// threads
		def addTwoInParallel() = Future.sequence(List(
			Future(addOne()),
			Future(addOne())
		)).map(_ => ())

		for {
			// add two (in parallel)
			() <- addTwoInParallel()

			// then double it (fire-and-forget)
			() <- state.sendTransform(_ * 2)

			// mutate the state and also return a result
			// (waits for the result, not fire-and-forget)
			formatted <- state.mutate { ref =>
				ref.set(ref.current + 3)
				s"The counter ended up being: ${ref.current}"
			}
		} yield formatted
	}

	def main() {
		println(Await.result(run(), Duration.Inf))
	}
}
```

Running this produces:

```
The counter ended up being: 7
```

Unlike actors, SequentialState instances are typically `private` and do not talk to each other directly. Since they encapsulate state, you typically use them to wrap the internal state of classes which need to be thread-safe.

For the full API see [SequentialState.scala](./src/main/scala/net/gfxmonk/sequentialstate/SequentialState.scala) (TODO: publish docs)

See [examples](./examples) for working examples.

## How do I install it?

TODO: packaging ;)

### What features does it have?

- Sequential execution:

This is the core of the actor concurrency model. Actors are not a unit of parallelism, they are a unit of sequentiality. One core benefit of actors is that you can have lots of them efficiently executing in parallel, however at the individual actor level _an actor is never doing two things at once_. For the duration of handling a given message, you are guaranteed that the actor is not doing anything else in parallel, meaning that you can manipulate an actor's private state without concern for race conditions.

 - Type safety

Every actor can receive an `Any`, and responses can only take the form of a `Future[Any]`. Part of the reason for this is that all messages are funneled through a single `receive` function which may accept many different types of messages, potentially depending on the state of the actor.

SequentialState actions are fully typed, there is no dynamic typing required (unless you want to introduce it).

- Backpressure:

Sending a SequentialState action (without waiting for its result) returns a Future[Unit] which resolves once the task has been accepted into the receiver's queue (but before the task has run). As long as senders wait for this future to resolve before sending additional messages, the number of actions enqueued on any given SequentialState will be bounded, providing backpressure and preventing work from piling up into unbounded queues. By blocking asynchronously via a Future[Unit], we avoid tying up threads with blocking calls (which can cause deadlocks).

Akka does not allow feedback on message sends, which necessitates explicit backpressure protocols on top of the actor model (e.g. as implemented by akka-streams).

### What _doesn't_ it have?

- (Possibly) remote actors:

Akka lets you send messages to both remote and local actors using the same interface. If you want a remote communication protocol, you should use one of those :)

- Heirarchichal supervisor strategies:

Actors define a heirarchy, primarily for supervision and error handling purposes. SequentialState provides no builtin mechanisms for this.

- High level pipeline DSL (a.k.a Reactive Streams):

SequentialState is a replacement for local actors, not for a fully fledged data pipeline like akka-streams, monix, etc. If your problem is well represented by a data pipeline, you should probably use one.
