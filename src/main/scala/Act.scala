class Actee[T](init: T, sched: Scheduler) {
	private val state = new Atomic[T](init)
	def enqueueSync(f: Function[T,T]
}

class Ref[T](init:T) = {
	private val v = init
	def get() = v
	def set(updated) {
		v = updated
	}
}

class SequentialState[T](init: T, sched: Scheduler) {
	// XXX volatile?
	private var state = new Ref(init)
	private var thread = new Lwt(sched)

	def mutate(fn: Function[Ref[T],R]):Future[Future[R]] = {
		thread.enqueue(Task { fn(state) })
	}

	def read(fn: Function[T,R]):Future[Future[R]] = {
		thread.enqueue(Task { fn(state.get) })
	}

	def mutateAsync(fn: Function[Ref[T],R]):Future[Future[R]] = {
		thread.enqueueAsync(Task { fn(state) })
	}
}

class SampleActorWordCount {
	val state = new Actee(0)
	def feed(line: String) = state.mutate { state =>
		val old = state.get
		state.set(state.get + line.split().size())
		println(s"updated word count from: $old -> ${state.get}")
	}

	def reset() = actee.mutate(_.set(0))
	def print() = actee.mutate(count => println(count.get))
}

class SampleActorLineCount {
	val state = new SequentialState(0)
	def inc() = state.mutate { state => state.set(state.get + 1) }
}


// Supervisor strategy: I guess just have an uncaughtHandler per LWT?
// You should be using Try instead of exceptions anyway...

class RequestHandler(stats: Stats) {
	val db = new DbPool()
	val fs = new FS()

	// Stats is an "Actor". But you know what?
	// nobody gives a shit. It has methods which return Future[Something],
	// so you can just call it. It might also have some non-future methods,
	// in which case they won't be mutating state...

	def request(req: Request): Future[Response] {
		// fire and forget
		stats.incrementPageCount(req.page) // Future[Unit]

		// chain futures
		db.fetchModel(req.params("id")).map { model =>
			renderPage(model)
		}
	}
}

class Lwt(sched: Scheduler) {
	private val state = new Atomic((Nil,None))
	val loop = new Runnable() {
		def pop():Future[_] {
			state.compareAndSet(s.tasks.pop)
		}

		def run() {
			while(true) {
				pop() match {
					case None => {
						// we've already been cleared!
					}
					case Some((task, p)) => {
						// evaluate this function on this fiber
						P.complete(Try {
							task()
						})
					}
				}
			}
		}
	}

	def enqueue(fun: Task[A,B]): Future[B] = {
		// TODO: reimplementing is probably more efficient
		Await.result(enqueueAsync(fun), Duration.Inf)
	}

	def enqueueAsync(fun: Task[A,B]): Future[Future[B]] = {
		var runningTask = null
		val queued = Future {
			tasks += task.runAsync
			runningTask = state.work
			if (runningTask == null) {
				// ...
			}
		}
		if (runningTask == null) {
			sched.execute(loop)
		}
		return queued
	}

	def loop() {
		while(true) {
		}
	}
}

object ActTest {
	def main() {
			
	}
}
