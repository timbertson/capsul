import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Success, Try}
// class Actee[T](init: T, sched: Scheduler) {
// 	private val state = new Atomic[T](init)
// 	def enqueueSync(f: Function[T,T]
// }
//
// class Ref[T](init:T) = {
// 	private val v = init
// 	def get() = v
// 	def set(updated) {
// 		v = updated
// 	}
// }

//class SequentialState[T](init: T, sched: Scheduler) {
//	// XXX volatile?
//	private var state = new Ref(init)
//	private var thread = new Lwt(sched)
//
//	def mutate(fn: Function[Ref[T],R]):Future[Future[R]] = {
//		thread.enqueue(Task { fn(state) })
//	}
//
//	def read(fn: Function[T,R]):Future[Future[R]] = {
//		thread.enqueue(Task { fn(state.get) })
//	}
//
//	def mutateAsync(fn: Function[Ref[T],R]):Future[Future[R]] = {
//		thread.enqueueAsync(Task { fn(state) })
//	}
//}
//
//class SampleActorWordCount {
//	val state = new Actee(0)
//	def feed(line: String) = state.mutate { state =>
//		val old = state.get
//		state.set(state.get + line.split().size())
//		println(s"updated word count from: $old -> ${state.get}")
//	}
//
//	def reset() = actee.mutate(_.set(0))
//	def print() = actee.mutate(count => println(count.get))
//}
//
//class SampleActorLineCount {
//	val state = new SequentialState(0)
//	def inc() = state.mutate { state => state.set(state.get + 1) }
//}
//
//
//// Supervisor strategy: I guess just have an uncaughtHandler per LWT?
//// You should be using Try instead of exceptions anyway...
//
//class RequestHandler(stats: Stats) {
//	val db = new DbPool()
//	val fs = new FS()
//
//	// Stats is an "Actor". But you know what?
//	// nobody gives a shit. It has methods which return Future[Something],
//	// so you can just call it. It might also have some non-future methods,
//	// in which case they won't be mutating state...
//
//	def request(req: Request): Future[Response] {
//		// fire and forget
//		stats.incrementPageCount(req.page) // Future[Unit]
//
//		// chain futures
//		db.fetchModel(req.params("id")).map { model =>
//			renderPage(model)
//		}
//	}
//}

case class UnitOfWork[A](fn: Function0[A]) {
	val promise = Promise[A]()
	def future = promise.future
	def run() = {
		try {
			promise.success(fn())
		} catch {
			case e:Throwable => {
				if (NonFatal(e)) {
          promise.failure(e)
				} else {
          throw e
				}
			}
		}
	}
}

object ThreadState {
	def popTask(state: ThreadState):(Option[UnitOfWork[_]],ThreadState) = {
		if (state.tasks.isEmpty) {
			(None, state)
		} else {
			val (head, tail) = state.tasks.dequeue
			(Some(head), new ThreadState(tail, state.waiters))
		}
	}

	def popWaiter(state: ThreadState):(Option[Function0[Unit]],ThreadState) = {
		if (state.waiters.isEmpty) {
			(None, state)
		} else {
			val (head, tail) = state.waiters.dequeue
			(Some(head), new ThreadState(state.tasks, tail))
		}
	}
}
class ThreadState(val tasks: Queue[UnitOfWork[_]], val waiters: Queue[Function0[Unit]]) {
	def enqueueTask(task: UnitOfWork[_]) = new ThreadState(tasks.enqueue(task), waiters)
	def enqueueWaiter(waiter: Function0[Unit]) = new ThreadState(tasks, waiters.enqueue(waiter))
	def length = tasks.length
}

class Lwt(sched: Scheduler, limit: Int) {

	private val state = Atomic(new ThreadState(Queue.empty[UnitOfWork[_]], Queue.empty[Function0[Unit]]))

	private val complete = null

	val loop = new Runnable() {
		private def park(): Unit = {
			// TODO, obvs...
		}

		def run() {
			var alive = true
			while(alive) {
				state.transformAndExtract(ThreadState.popTask) match {
					case None => {
						alive = false
						park()
					}
					case Some(task) => {
						// evaluate this function on this fiber
						task.run()

						// notify the first waiter, if there is one.
						// Note that we want to do this before we check `pop()` again,
						// so it has a chance to enqueue a new task
						state.transformAndExtract(ThreadState.popWaiter).foreach(_())
					}
				}
			}
		}
	}

	def enqueue[A](fun: Function0[A]): Future[A] = {
		// TODO: reimplementing is probably more efficient
		Await.result(enqueueAsync(fun), Duration.Inf)
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork(fun)

		// try an immediate enqueue:
		val done = state.transformAndExtract { state =>
			if (state.length < limit) {
				(true, state.enqueueTask(task))
			} else {
				(false, state)
			}
		}

		if (done) {
			//easy:
			Future.successful(task.future)
		} else {
			// slow mode: make a promise and complete it once we've successfully enqueued
      val enqueued = Promise[Future[A]]()
      def enqueue():Unit = {
        val done = state.transformAndExtract { state =>
          if (state.length < limit) {
            (true, state.enqueueTask(task))
          } else {
            (false, state.enqueueWaiter(enqueue))
          }
        }

        if (done) {
          enqueued.success(task.future)
        }
			}
			enqueue()
			enqueued.future
		}
	}
}

object ActTest {
	def main() {
			
	}
}
