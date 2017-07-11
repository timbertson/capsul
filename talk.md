Overview:

 - why (not) actors

 - why this

 - abstraction fit
  - the bigger an abstraction, the less chance it actually fits your needs well

 - what's it good for?
   - stubborn people who don't want to make everything an actor


-------------

Doing some kafka event streaming work lately, where we consume kafka messages, put them through a mostly pure pipeline transformation, do as much as we can in parallel, and then publish the results of our pipeline back into a separate kafka stream.

This is a perfect fit for akka-streams or monix, but I couldn't shake the thought that it was kind of weird to layer this type-safe pure DSL on top of actors, whose whole thing is that they're basically untyped balls of mutable state.


I got looking into reactive-streams, which is this collaborative standard that both akka-streams and monix implement. The main purpose of it seems to be about implementing backpressure. Which is a fancy word for "don't give me more shit than I can handle". And.. It's a surprisingly hard thing to do, if you want to do it efficiently.

The way I've always thought about it is basically with threads and queues - a queue has a capacity, and it'll block if it's not ready for you. That's natural backpressure, because the code generating the work literally cannot do anything else. But threads are relatively expensive, and blocking sucks. In particular, blocking combined with a capped thread pool can easily lead to deadlocks.


Akka, and actors in general, have this gung ho attitute. Send a message! It's instantaneous! It'll _probably_ get there!

And you know, on a local system it _will_ arrive, but then you have the issue that there's no backpressure. If my actor is processing a message every 5 milliseconds and sending it on to the next actor who can only process a message every 6 milliseconds, you're eventually going to run out of memory, because actor mailboxes are infinite but RAM is sadly not.

If you want to see that your message made it, you can wait for a response, as a promise. But then that's slower, because you're waiting for one task to be done before you fire off the next one. With tasks that can be buffered to smooth out load spikes, you're waiting unnecessarily long before buffering the next item.

And I've tried writing a future-based API with queues, where you send a task and it normally returns immediately, but if the queue is full it blocks. Which is efficient in general, but can still lead to deadlocks.


So what if nothing blocked, but we could still apply backpressure when we're not ready to accept a new task?

Sync: doSomething() => T
Async: doSomething() => Future[T]
Aync with explicit backpressure: Future[Future[T]]

I mean, it's awkward. But break it down:

doSomething()
	// the message has been sent
	.foreach { future =>
		// the message has been accepted by the receiver
		future.foreach { result =>
			// finally, a T!
		}
	}

Super freakin' awkward, right?


---

OK, let's talk actors



Akka streams addresses backpressure by flipping the traditional pipeline on its head - you don't push items into the pipeline as fast as you can, it's actually the tail of the pipeline saying "hey, I've got space for <n> items, send some down!". That request flows upwards through the pipeline, and on receiving this request, the source will dutifully send only `<n>` items into the start of the pipeline. If a stage is buffering or collapsing items, it can send additional requests for work upstream until it gets enough to generare the work that its downstream requested.

Which is all well and good for a pipeline setup, but what about ad-hoc actors just talking to whoever they please?
