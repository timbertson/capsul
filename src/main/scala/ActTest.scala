import ThreadState.EnqueueResult
import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util._


class SampleActorWordCount(implicit sched: ExecutionContext) {
	val state = SequentialState(0)
	def feed(line: String) = state.mutate { state =>
		val old = state.get
		state.set(state.get + line.split("\\w").length)
		if (old > 12380) {
			// XXX hacky
//			println(s"updated word count from: $old -> ${state.get}")
		}
	}

	def reset() = state.mutate(_.set(0))
	def get() = state.read(identity)
	def print() = state.read(println)
}

class SampleCountingActor(bufLen: Int)(implicit ec: ExecutionContext) {
	val state = SequentialState(bufLen = bufLen, v = 0)
	def inc() = state.mutate { state =>
		state.set(state.get + 1)
	}
	def get() = state.read(identity)
}

class PingPongActor(bufLen: Int)(implicit ec: ExecutionContext) {
	var peer: PingPongActor = null
	val state = SequentialState[List[Int]](bufLen = bufLen, v = Nil)
	def setPeer(newPeer: PingPongActor) {
		peer = newPeer
	}
	def ping(n: Int): Future[Future[Unit]] = {
		val sent = state.mutate { state =>
			state.set(n :: state.get)
		}
		sent.flatMap { (result:Future[Unit]) =>
			if (n == 1) {
				Future.successful(result)
			} else {
				val sent = peer.ping(n-1)
				sent.map { (_:Future[Unit]) => Future.successful(()) }
			}
		}
	}

	def get():Future[List[Int]] = state.read(identity).flatMap(identity)
}

object PingPongActor {
	def run(n: Int, bufLen: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val a = new PingPongActor(bufLen)
		val b = new PingPongActor(bufLen)
		a.setPeer(b)
		b.setPeer(a)
		a.ping(n).flatMap { (result:Future[Unit]) =>
			a.get().zip(b.get()).map { case (a,b) => a.size + b.size }
		}
	}
}


class PipelineStage[T,R](
	bufLen: Int,
	process: Function[T,Future[R]],
	handleCompleted: Function[List[Try[R]], Future[_]]
)(implicit ec: ExecutionContext) {
	val state = SequentialState(bufLen = bufLen, v = new State())

	class State {
		val queue = new mutable.Queue[Future[R]]()
		var outgoing: Option[Future[_]] = None
	}

	private def _drainCompleted(state: State): Unit = {
		// called any time a future completes, either an item in `queue` or `outgoing`
		if (state.outgoing.isDefined) {
			return
		}

		val ret = new mutable.ListBuffer[Try[R]]()
		while(true) {
			state.queue.headOption match {
				case Some(f) if f.isCompleted => {
					ret.append(f.value.get)
					state.queue.dequeue()
				}
				case _ => {
					if (!ret.isEmpty) {
						// we have some items to produce
						val fut = handleCompleted(ret.toList)
						state.outgoing = Some(fut)
						fut.onComplete(_ => this.state.read { state =>
							state.outgoing = None
							// set up another drain when this outgoing batch is done
							_drainCompleted(state)
						})
					}
					return
				}
			}
		}
	}

	private def drainCompleted() {
		state.read(_drainCompleted).flatMap(identity)
	}

	def enqueue(item: T):Future[Unit] = {
		state.read { state =>
			_drainCompleted(state)

			if (state.queue.size < bufLen) {
				val fut = process(item)
				state.queue.enqueue(fut)
				fut.onComplete((_:Try[R]) => drainCompleted())
				Future.successful(())
			} else {
				// queue full, wait for the first item to be done and then try again
				val promise = Promise[Unit]()
				state.queue.head.onComplete((_:Try[R]) => enqueue(item).foreach(promise.success))
				promise.future
			}
		}.flatMap(identity).flatMap(identity)
	}
}

object Pipeline {
	def run(stages: Int, len:Int, bufLen: Int, timePerStep: Int, jitter: Float)(implicit ec: ExecutionContext): Future[Int] = {
		val source = Iterator.continually { () }.take(len)
		val sink = SequentialState(bufLen, v = 0)
		def finalize(batch: List[Try[Unit]]): Future[Unit] = {
			sink.mutate { state =>
				state.set(state.get + batch.size)
			}.map((_:Future[Unit]) => ())
		}
		def process(item: Unit) = {
			Future {
				val sleepTime: Int = timePerStep + (Random.nextFloat() * (timePerStep.toFloat) * jitter).toInt
				Thread.sleep(sleepTime)
			}
		}

		def connect(sink: Function[List[Try[Unit]], Future[Unit]], stages: Int):Function[List[Try[Unit]], Future[Unit]] = {
			val stage = new PipelineStage(bufLen, process, sink)
			def handleBatch(batch: List[Try[Unit]]): Future[Unit] = {
				Future.sequence(batch.map(item => stage.enqueue(item.get)))
					.map((_:List[Unit]) => ())
			}
			if (stages == 0) {
				handleBatch
			} else {
				connect(handleBatch, stages - 1)
			}
		}

		val fullPipeline = connect(finalize, stages)
		def pushWork():Future[Unit] = {
			if (source.hasNext) {
				val item = source.next
				fullPipeline(List(Success(item))).flatMap((_:Unit) => pushWork())
			} else {
				Future.successful(())
			}
		}

		pushWork().flatMap { (_:Unit) =>
			sink.read(identity).flatMap(identity)
		}
	}
}

object ActorExample {
	import scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def simpleCounter(bufLen: Int, limit: Int): Future[Int] = {
		val lineCount = new SampleCountingActor(bufLen=bufLen)
		def loop(i: Int): Future[Int] = {
			lineCount.inc().flatMap { complete: Future[Unit] =>
				val nextI = i+1
				if (nextI == limit) {
					complete.flatMap { (_:Unit) =>
						lineCount.get().flatMap(identity)
					}
				} else {
					loop(nextI)
				}
			}
		}
		loop(0)
	}

	def countWithSequentialStates(bufLen: Int, lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleActorWordCount()
		val lineCount = new SampleCountingActor(bufLen = bufLen)
		def loop(): Future[(Int, Int)] = {
			if (lines.hasNext) {
				for {
					numWords: Future[Unit] <- wordCounter.feed(lines.next())
					numLines: Future[Unit] <- lineCount.inc()
					// XXX potentially no need to wait until the last round?
					// result <- numWords.zip(numLines).flatMap(_ => loop())
						result <- (if (lines.hasNext) loop() else numWords.zip(numLines).flatMap(_ => loop()))
				} yield result
			} else {
//				println("final line!")
				for {
					words <- wordCounter.get()
					lines <- lineCount.get()
					result <- words.zip(lines)
				} yield result
			}
		}

		loop()
	}

	def time(name:String, impl: => Future[_]) = {
		val start = System.currentTimeMillis()
//		println("Start")
		val f = impl
//		println("(computed)")
		val result = Await.result(f, Duration.Inf)
		val end = System.currentTimeMillis()
		val duration = end - start
//		println("Done")
		println(s"implementation $name took $duration ms to calculate $result")
	}

	def repeat(n: Int)(f: => Unit) {
		var attempt = n
		while(attempt>0) {
			f
			attempt -= 1
		}
	}

	def run(): Unit = {
		val repeat = this.repeat(20) _
		val bufLen = 4

		// count lines (2 actors in parallel)
		repeat {
			val numLines = 3000
			time("SequentialState: word count", countWithSequentialStates(bufLen = bufLen, lines=makeLines(numLines)))
		}

		// simple counter (1 actor in serial)
		repeat {
			val countLimit = 10000
			time("SequentialState: simple counter", simpleCounter(bufLen = bufLen, limit=countLimit))
		}

		repeat {
			val countLimit = 1000000
			time("while loop counter", {
				var i = 0
				while (i < countLimit) {
					i += 1
				}
				Future.successful(i)
			})
		}

		repeat {
			time("SequentialState: explosive ping", PingPongActor.run(1000, bufLen = bufLen))
		}

		repeat {
			time("SequentialState: n=5, t=0, x100 pipeline", Pipeline.run(
				stages = 5,
				len = 100,
				bufLen = bufLen,
				timePerStep = 0,
				jitter = 0.5f
			))
		}

		repeat {
			time("SequentialState: n=5, t=5, x20 pipeline", Pipeline.run(
				stages = 5,
				len = 20,
				bufLen = bufLen,
				timePerStep = 5,
				jitter = 0.5f
			))
		}
	}
}


object ActTest {
	def main() {
		ActorExample.run
	}
}
