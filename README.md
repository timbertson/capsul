![Capsul](./res/logo.png)

(previously known as `SequentialState`)

Capsul is a minimal, type-safe alternative to (some of) akka. Features:

 - Simple (less than 1k LOC)
 - Familiar, functional & type-safe API
 - Backpressure built in
 - Efficient and non-blocking parallel execution (around the same performance as Akka)

**Quick links:**

 - [API docs](https://timbertson.github.io/sequentialstate/api/net/gfxmonk/sequentialstate/)
 - [examples](./examples/src/main/scala/net/gfxmonk/sequentialstate/examples) for working examples.

I also presented an introduction to Capsul (when it was called SequentialState) at the [Melbourne Scala User Group](https://www.meetup.com/en-AU/Melbourne-Scala-User-Group/) in August 2017, you can see the [video](https://youtu.be/WsE4S8qDjgk) and [slides](https://timbertson.github.io/sequentialstate/talk/) (with speaker notes).

## Why should I use Capsul?

_Capsul provides the same concurrency model as local actors, plus type safety and builtin support for non-blocking backpressure. It achieves this by simply encapsulating state, doing away with most other features of actors._

Parallel programming (using locks, mutexes, etc) is still a surprisingly error-prone task even in modern scala. While you rarely need to implement your own concurrency primitives, you do need to use the builtin ones correctly. That isn't always straightforward or efficient, and frequently requires diligence that cannot be enforced by the compiler.

I liked the simple concurrency guarantees of actors, but found that akka was unfitting for many purposes. I realised that I didn't want my system to be composed of actors, I was happy with _mostly_ stateless functional programming. But for the places where I must maintain state, I needed something better than a lock.

Also, when learning about akka-streams and monix (and the reactive streams protocol), I was dismayed to realise that plain akka provides no building blocks for dealing with backpressure and preventing overload.


## Simple example:

```scala
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import net.gfxmonk.sequentialstate._

object Main {
	def run(): Future[String] = {
		// create a Capsul[Int]
		val state = Capsul(0)

		def addOne() = state.sendTransform(_ + 1)

		// This is a bit unnecessary, just showing that
		// state can be safely accessed from two different
		// threads
		def addTwoConcurrently() = Future.sequence(List(
			Future(addOne()),
			Future(addOne())
		)).map(_ => ())

		for {
			// add two (fire and forget)
			() <- addTwoConcurrently()

			// then double it (also fire-and-forget)
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

Unlike actors, Capsul instances are typically `private` and do not talk to each other directly. Since they encapsulate state, you typically use them to wrap the internal state of classes which need to be thread-safe.

## How do I use it?

Add to build.sbt:

```
libraryDependencies += "net.gfxmonk" %% "sequentialstate" % "0.2.0"
```

### What features does it have?

- Sequential execution:

This is the core of the actor concurrency model. Actors are not a unit of parallelism, they are a unit of sequentiality. One core benefit of actors is that you can have lots of them efficiently executing in parallel, however at the individual actor level _an actor is never doing two things at once_. For the duration of handling a given message, you are guaranteed that the actor is not doing anything else in parallel, meaning that you can manipulate an actor's private state without concern for race conditions.

 - Type safety

Every actor can receive an `Any`, and responses can only take the form of a `Future[Any]`. Part of the reason for this is that all messages are funneled through a single `receive` function which may accept many different types of messages, potentially depending on the state of the actor.

Capsul actions are fully typed, there is no dynamic typing required (unless you want to introduce it).

- Backpressure:

Sending a Capsul an action (without waiting for its result) returns a Future[Unit] which resolves once the task has been accepted into the receiver's queue (but before the task has run). As long as senders wait for this future to resolve before sending additional messages, the number of actions enqueued on any given Capsul will be bounded, providing backpressure and preventing work from piling up into unbounded queues. By blocking asynchronously via a Future[Unit], we avoid tying up threads with blocking calls (which can cause deadlocks).

Akka does not allow feedback on message sends, which necessitates explicit backpressure protocols on top of the actor model (e.g. as implemented by akka-streams).

### What _doesn't_ it have?

- (Possibly) remote actors:

Akka lets you send messages to both remote and local actors using the same interface. If you want a remote communication protocol, you should use one of those :)

- Heirarchichal supervisor strategies:

Actors define a heirarchy, primarily for supervision and error handling purposes. Capsul provides no builtin mechanisms for this.

- High level pipeline DSL (a.k.a Reactive Streams):

Capsul is a replacement for local actors, not for a fully fledged data pipeline like akka-streams, monix, etc. If your problem is well represented by a data pipeline, you should probably use one.

### How does backpressure work?

Synchronous backpressure is reasonably straightforward with a traditional bounded, blocking queue. Each component has a queue of tasks which you can add to. Once that queue reaches capacity, it won't allow more tasks to be enqueued. This is done by blocking the thread trying to enqueue more work until space is available.

To avoid actually tying up a thread (potentially leading to deadlocks), Capsul blocks the caller by always returning a `StagedFuture` which won't be `accepted` until the work has successfully been enqueued. Space is freed up in a queue after a task has run. **Note** that this requires code enqueueing work to be "good citizens" -- you should not enqueue more work until the previous work has been accepted.

This solution is all you need for synchronous tasks - once the item has been run, it is complete. But for asynchronous tasks (which run some code that creates and returns a `Future[T]`), there are two stages to each task:

 - (synchronous) **started**; the running of the task which creates a `Future[T]`
 - (asynchronous) **completed**, the point when the `Future[T]` succeeds (or fails)

Capsul will naturally apply backpressure to the _starting_ of asynchronous tasks, since that part runs synchronously.

But for the asynchronous completion of the work, we need to make _started-but-incomplete_ tasks still occupy a slot in the work queue. This ensures that (for example) a component with a capacity of 10 queued tasks will prevent further tasks from being enqueued once it has:

 - 10 enqueued (not executed) tasks, or
 - 9 enqueued tasks plus one run-but-incomplete asynchronous task, or
 - 5 enqueued tasks plus 5 run-but-incomplete asynchronous task, or
 - 10 run-but-incomplete asynchronous tasks, (etc...)

To get this behaviour, you must call the `Async` or `Staged` versions of the methods on a `Capsul` object - e.g. `mutateAsync`, `accessAsync`, `mutateStaged`, etc. Using the non-async variants is typically awkward enough to make this obvious, as you will end up with a nested `StagedFuture[Future[T]]` which requires flattening - when you see this, you should usually use the `Async` variant instead.

The only difference between these two variants is that `Async` (tasks returning a plain `Future`) occupy a task slot until they complete, while `Staged` (tasks returning a `StagedFuture`) occupy a task slot until they are accepted (at which point they will be occupying a task slot in some other `Capsul` object's queue, typically). The `Staged` variant is useful for chaining together multiple `Capsul` actions.
