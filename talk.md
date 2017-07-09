What do Actors do:

[The good]

- serial execution: an actor is _never_ doing two things concurrently
  - this makes logic very simple - if you don't expose your state, you never have to worry about concurrent modifications.
  - I don't know of an easy way to get this without manual locks, which are not very efficient

- buffered message sends
  - messages can be enqueued immediately if the actor is busy, leading to good throughput and low blocking

[The bad]

- Any => Unit
  - Every actor receives any message. And returns nothing. Dynamic typing combined with unidirectional communication
  - Where's my types? ಠ_ಠ

- The buffer is _infinite_
  - Can drop messages, but can't block the sender. If I don't want incoming load to overwhelm me, I need to implement my own rate limiting, and that's hard (thus akka-streams)

- A lot of abstraction
  - Conceptually actors may be simple, but using them is not always straightforward. Actor systems, paths and props all need to be understood to use actors correctly. You can't just `new()` an actor, you have to get an actor system and ask _it_ to invoke an actor for you with an instance of `Props`.
  - Oh my god there's so much config. If you have that much stuff to configure, your system might be doing too much

- Infectious
  - If you want to talk to actor, you're going to have a hard time not being an actor

[The indifferent]

- context.become()
  - actors can be in different states. I don't much like state, but maybe it's better than managing state manually?

- supervisation strategies
  - I love exhaustive error handling. I'm not convinced this is a good solution, but it's a decent default

- remote actors
  - cool? I've never had much use for this, so I can't really comment.

------------------------

I'll tell you what I really want (what I really really want):
- isolation: locking is Too Hard (TM)
- backpressure!
- discourage state. Put it in a little box that you only open when yo ureally need to.



So I wrote me some code.

It's got two abstractions. They're itty-bitty:

SequentialExecutor:

  Kind of like an actor's mailbox. Accepts tasks (functions) to be run. Stores them in a (finite) buffer. Will never schedule two tasks concurrently. But it's not a real thread - it's lock-free and asynchronous. Also ordered - if you enqueue `a` then `b`, `b` is guaranteed to only run after `a` has completed.

  The result of a enqueue(fn: () => T) is Future[Future[T]].

  The outer future resolves when your task is enqueued - once that's done, you can power ahead and enqueue more work without overloading anything.
  If you need the result of your task, you can additionally wait for the inner future. Two layers of Future provides flexibility, but also a bit confusing.

SequentialState:

  Has a SequentialExecutor and a mutable reference to some state.

  Exposes a bunch of methods to access and modify that state.
  
  If you just need to send something, you can _enqueue_ a state change, and you'll get a Future[Unit]. Once it's done, go ahead and enqueue more changes. They'll all be run on the same SequentialExecutor, so you get actor-like isolation.

  ```
  // fire off an `increment` action
  state.sendMap (_ + 1) // => Future[Unit], completes when the action is enqueued
  ```

  If you need to wait for a change to actually be complete (e.g. you need its result), there are methods for that too:

  ```
  // increment and return the current value
  state.awaitMutate { state =>
    state.set(state.current + 1)
    state.current
  }
  // => Future[Int]
  ```

Hides the awkward `Future[Future[_]]` type, with convenience functions for waiting on either the send or the result, because you rarely need to care about both (although you can with `rawMutate`).


Upshots:

 - Small abstraction - it's just futures.
 - It's just for concurrent-safe state manipulation. It doesn't combine state with code with a supervisor hierarchy.
 - Completely lock-free & asynchronous - no blocking anywhere.
 - It's _encapsulated_. Consumers see a plain class with async methods which return Futures (and whatever other methods you like!). They don't have to know how to talk actor.
 - You can mix & match SequentialState with whatever other concurrent-safe abstractions fit - e.g. a shared cache
 - fast enough* (more on this later)

Downshots: (that's a word)
 - Error management is just what Future gives you. There's no such thing as a "failed component" like in actors, it's just an operation which fails. It's up to you how you want to deal with that.

---------------------------------------------

The awkward thing about actors is how do you talk to them? They're off in their own system - you can send them messages easily enough, but they can't answer you because you're just some code, you don't belong to an actor.


Here's how you might implement a thread-safe counting actor:

```
class CounterActor extends Actor {
	import CounterActor._

	var i = 0

	def receive = {
		case Increment => i += 1
		case Decrement => i -= 1
		case ReturnCount(p) => p.success(i)
	}
}

object CounterActor {
	case object Increment
	case object Decrement
	case class ReturnCount(p: Promise[Int])

	def run()(implicit system: ActorSystem, ec: ExecutionContext): Future[Unit] = {
		val counter = system.actorOf(Props[CounterActor])
		counter ! Increment
		counter ! Increment
		counter ! Decrement
		val result = Promise[Int]()
		counter ! ReturnCount(result)
		result.future.map { i =>
			println(s"I just counted to $i in an elaborate manner")
		}
	}
}
```

Firstly, `CounterActor` is effectively a dynamic object. It has no API to speak of, you just throw things at it and hope it doesn't crash.
(and if it does, you won't know!)

Also because it doesn't have a return channel, you have to send it weird messages like "here's a promise, please complete it with your current state when you get this".

Alternatively, here's a counter with sequential state:

```
class CounterState()(implicit ec: ExecutionContext) {
	private val state = SequentialState(0)
	def inc() = state.sendMap(_+1)
	def dec() = state.sendMap(_-1)
	def current = state.current
}

object CounterState {
	def run()(implicit ec: ExecutionContext): Future[Unit] = {
		val counter = new CounterState()
		counter.inc()
		counter.inc()
		counter.dec()
		counter.current.map { i =>
			println(s"I just counted to $i in an elaborate manner")
		}
	}
}
```

That's... pretty un-weird. It's not idiomatic use, because it's just throwing stuff at `counter` like akka does, with no chance of the receiver saying "hey hey wait, I can't keep up with this crap". If this were a less trivial program, you'd need to wait for messages to be enqueued so you don't overwhelm the receiver. That can make things more involved:

```
	def runWithBackpressure()(implicit ec: ExecutionContext): Future[Unit] = {
		val counter = new CounterState()
		for {
			() <- counter.inc()
			() <- counter.inc()
			() <- counter.dec()
			i <- counter.current
		} yield println(s"I just counted to $i in an elaborate manner")
	}
```

.. but it's not _that_ crazy. You just follow the types. When you `inc()`, you get back a `Future[Unit]` so you should wait for that before throwing more stuff at the counter. And `current` is a `Future[Int]`, so you're gonna have to wait for that if you want the number out.



-------------------------------

Performance: not very scientific, just an average of 20x runs (ignoring the first run for warm-up)

Some microbenchmarks:

* perform 10,000 "increment" actions in a tight loop:
  - akka: 34ms
  - seq: 54ms
  - seq-with-backpressure: 14ms

...I'm assuming memory pressure / GC is the slow thing here, which goes to show how hard it is to compare with akka directly.

* send 2000 items through an N-stage pipeline triggering parallel tasks on M process threads:

2 stages, 2 threads:
('Akka:', 54.888888888888886)
('SequentialState:', 112.44444444444444)
('Monix:', 44.0)

8 stages, 8 threads:
('Akka:', 80.88888888888889)
('SequentialState:', 454.55555555555554)
('Monix:', 102.22222222222223)


2 stages, 8 threads: (contention on state serialization)
('Akka:', 79.88888888888889)
('SequentialState:', 158.22222222222223)
('Monix:', 81.22222222222223)

8 stages, 2 threads: (contention on threads)

('Akka:', 135.44444444444446)
('SequentialState:', 735.2222222222222)
('Monix:', 102.33333333333333)


Overall seq seems ~2x slower than akka in terms of overhead. Not too bad for a weekend project. There are a couple opportunities for speedups which I'm still investigating.

I would have loved if mine was faster. But even if it's slower, the simpler abstraction is worth it for many purposes. If you want to protect a piece of state for thread-safe mutation, "put it in an actor" is an incredibly inconvenient encapsulation.






/////
//
//Other notes...

Act: a single function with an actor's isolation guarantees
Actee: a chunk of data which you can schedule work on

How do results come back? How does blocking work?

Concurrency font: implement backpressure via tokens
- get token
- perform work
- return token

Very simple, could be combined with a tell-only act (e.g. A => Unit)


Perhaps combine a font with an unbounded Act(ee).


