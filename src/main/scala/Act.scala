import java.nio.charset.Charset

import ThreadState.EnqueueResult
import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Success, Try}
// class Actee[T](init: T, sched: Scheduler) {
// 	private val state = new Atomic[T](init)
// 	def enqueueSync(f: Function[T,T]
// }

class Ref[T](init:T) {
 	@volatile private var v = init
 	def get = v
 	def set(updated:T) {
 		v = updated
 	}
 }

object SequentialState {
	def apply[T](v: T)(implicit scheduler: ExecutionContext) = new SequentialState[T](v, scheduler)
}

class SequentialState[T](init: T, sched: ExecutionContext) {
	// XXX volatile?
	private val state = new Ref(init)
	private val thread = new Lwt(sched, bufferSize = 10)

	def mutate[R](fn: Function[Ref[T],R]):Future[Future[R]] = {
		thread.enqueueAsync(() => fn(state))
	}

	def set[R](updated: T):Future[Future[Unit]] = {
		thread.enqueueAsync(() => state.set(updated))
	}

	def read[R](fn: Function[T,R]):Future[Future[R]] = {
		thread.enqueueAsync(() => fn(state.get))
	}
}

class SampleActorWordCount(implicit sched: ExecutionContext) {
	val state = SequentialState(0)
	def feed(line: String) = state.mutate { state =>
		val old = state.get
		state.set(state.get + line.split("\\w").length)
		Thread.sleep(1000)
		if (old > 12380) {
			// XXX hacky
//      println(s"updated word count from: $old -> ${state.get}")
		}
	}

	def reset() = state.mutate(_.set(0))
	def get() = state.read(identity)
	def print() = state.read(println)
}

class SampleActorLineCount(implicit ec: ExecutionContext) {
	val state = SequentialState(0)
	def inc() = state.mutate { state => state.set(state.get + 1) }
	def get() = state.read(identity)
}

object ActorExample {
	import scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def countWithSequentialStates(lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleActorWordCount()
		val lineCount = new SampleActorLineCount()


		def loop(): Future[(Int, Int)] = {
      println("loop")
			if (lines.hasNext) {
				for {
					numWords: Future[Unit] <- wordCounter.feed(lines.next())
					numLines: Future[Unit] <- lineCount.inc()
					// XXX potentially no need to wait until the last round?
//					result <- numWords.zip(numLines).flatMap(_ => loop())
						result <- (if (lines.hasNext) loop() else numWords.zip(numLines).flatMap(_ => loop()))
				} yield result
			} else {
//        println("final line!")
				for {
					words <- wordCounter.get()
					lines <- lineCount.get()
					result <- words.zip(lines)
				} yield result
			}
		}

		loop()
	}

	def time(name:String, impl: => Future[(Int,Int)]) = {
		val start = System.currentTimeMillis()
//		println("Start")
		val f = impl
//		println("(computed)")
		val counts = Await.result(f, Duration.Inf)
		val end = System.currentTimeMillis()
		val duration = end - start
//		println("Done")
		println(s"implementation $name took $duration ms to calculate $counts")
	}

  def run(): Unit = {
  	val numLines = 30
  	var attempt = 1
  	while(attempt > 0) {
      time("SequentialState", countWithSequentialStates(makeLines(numLines)))
      attempt -= 1
		}
  }
}

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
	class EnqueueResult(val success: Boolean, val wasRunning: Boolean) {
		def shouldSchedule = success && !wasRunning
	}

	// pre-bind the combinations we need
	val ENQUEUED_ALREADY_RUNNING = new EnqueueResult(success = true, wasRunning = true)
	val ENQUEUED_NOT_RUNNING = new EnqueueResult(success = true, wasRunning = false)
	val ENQUEUE_REJECTED = new EnqueueResult(success = false, wasRunning = true)

	object EnqueueResult {
		def successful(running: Boolean) = {
			if (running) ENQUEUED_ALREADY_RUNNING else ENQUEUED_NOT_RUNNING
		}

		def rejected = ENQUEUE_REJECTED
	}


	def popTask(state: ThreadState):(Option[UnitOfWork[_]],ThreadState) = {
		if (state.tasks.isEmpty) {
			(None, state.park())
		} else {
			val (head, tail) = state.tasks.dequeue
			(Some(head), new ThreadState(tail, state.waiters, state.running))
		}
	}

	def popWaiter(bufferSize: Int, state: ThreadState):(Option[Waiter[_]],ThreadState) = {
		if (state.waiters.isEmpty || state.tasks.size >= bufferSize) {
			(None, state)
		} else {
			val (head, tail) = state.waiters.dequeue
			(Some(head), new ThreadState(state.tasks.enqueue(head.task), tail, state.running))
		}
	}

	def empty = new ThreadState(Queue.empty[UnitOfWork[_]], Queue.empty[Waiter[_]], false)

	def start(state: ThreadState): (Boolean, ThreadState) = {
		(!state.running, new ThreadState(state.tasks, state.waiters, true))
	}
}

class ThreadState(val tasks: Queue[UnitOfWork[_]], val waiters: Queue[Waiter[_]], val running: Boolean) {
	import ThreadState._

	def enqueueTask(task: UnitOfWork[_]): ThreadState = new ThreadState(tasks.enqueue(task), waiters, running)

	def enqueueWaiter(waiter: Waiter[_]): ThreadState = new ThreadState(tasks, waiters.enqueue(waiter), running)

	def length = tasks.length

	def park() = {
		if (running) {
			new ThreadState(tasks, waiters, running)
		} else {
			this
		}
	}
}

class Waiter[A](
	val task: UnitOfWork[A],
	promise: Promise[Future[A]]) {
	def enqueued() {
//		println("waiter enqueued!")
    promise.success(task.future)
	}
}

class Lwt(sched: ExecutionContext, bufferSize: Int) {
	private val state = Atomic(ThreadState.empty)

	val workLoop = new Runnable() {
		println("running")
		private def popWaiter(state: ThreadState) = ThreadState.popWaiter(bufferSize, state)
		def run() {
			// XXX tailrec?
			// XXX we may want to park this thread after at least every <n> tasks to prevent starvation.
			// If we do, who will schedule it again?
			var taskCount = 0
			def loop(): Unit = {
				val task = state.transformAndExtract(ThreadState.popTask)
//				println(s"I'ma loop ($taskCount), on thread " + Thread.currentThread().getId() + ", task = " + task)
				task match {
					case None => {
						println(s"parking (after $taskCount tasks on thread ${Thread.currentThread().getId()}")
					}
					case Some(task) => {
            taskCount += 1
            // evaluate this function on this fiber
						println(s"running $taskCount task on thread ${Thread.currentThread().getId()}")
            task.run()

            // and promote a waiting task, if any
            state.transformAndExtract(popWaiter).foreach { waiter =>
            	println("popped a waiter!")
            	waiter.enqueued()
						}
            loop()
					}
				}
			}
			loop()
		}
	}

	def enqueueSync[A](fun: Function0[A]): Future[A] = {
		// TODO: reimplementing is probably more efficient
		Await.result(enqueueAsync(fun), Duration.Inf)
	}

	private def autoSchedule(enqueueResult: EnqueueResult): Boolean = {
		if(enqueueResult.shouldSchedule) {
			println("scheduling")
			sched.execute(workLoop)
		}
		enqueueResult.success
	}

	def enqueueAsync[A](fun: Function0[A]): Future[Future[A]] = {
		val task = UnitOfWork(fun)

		// try an immediate enqueue:
		val done = autoSchedule(state.transformAndExtract { state =>
			if (state.tasks.length < bufferSize) {
				(EnqueueResult.successful(state.running), state.enqueueTask(task))
			} else {
				assert(!state.running)
				(EnqueueResult.rejected, state)
			}
		})

		if (done) {
			//easy mode:
			Future.successful(task.future)
		} else {
			// slow mode: make a promise and hop in the waiting queue if
			// tasks is (still) full
      val enqueued = Promise[Future[A]]()
			val waiter = new Waiter[A](task, enqueued)
			val done = autoSchedule(state.transformAndExtract { state =>
				if (state.length < bufferSize) {
//					println("enqueued on retry")
					(EnqueueResult.successful(state.running), state.enqueueTask(task))
				} else {
//					println("waiter enqueued")
					assert(!state.running)
					(EnqueueResult.rejected, state.enqueueWaiter(waiter))
				}
			})
			if (done) {
				waiter.enqueued()
			}
			enqueued.future
		}
	}
}

object ActTest {
	def main() {
		ActorExample.run
	}
}
