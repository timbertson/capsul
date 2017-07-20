import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent._
import scala.util._

import akka.actor.{ActorSystem,Actor}
import akka.stream.{Materializer,ActorMaterializer}

object Sleep {
	def jittered(base: Float, jitter: Float) = {
		val jitterMs = (Random.nextFloat() * base * jitter)
		val sleepMs = Math.max(0, (base + jitterMs).toInt)

		// var limit = sleepMs
		// while(limit > 0) {
		// 	limit -= 1
		// }
		Thread.sleep(sleepMs)
	}
}

class SampleActorWordCount(implicit sched: ExecutionContext) {
	val state = SequentialState(0)
	def feed(line: String) = state.sendTransform(_ + line.split("\\w").length)
	def reset() = state.sendSet(0)
	def current = state.current
}

class SampleCountingActor()(implicit ec: ExecutionContext) {
	val state = SequentialState(v = 0)
	def inc() = state.sendTransform(_ + 1)
	def current = state.current
}

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
	sealed trait Message
	case object Increment extends Message
	case object Decrement extends Message
	case class ReturnCount(p: Promise[Int]) extends Message

	def run(n: Int)(implicit system: ActorSystem, ec: ExecutionContext): Future[Int] = {
		import akka.actor.Props
		val counter = system.actorOf(Props[CounterActor])
		var limit = n
		while(limit > 0) {
			counter ! Increment
			limit -= 1
		}
		val result = Promise[Int]()
		counter ! ReturnCount(result)
		result.future
	}
}

class CounterState()(implicit ec: ExecutionContext) {
	private val state = SequentialState(0)
	def inc() = state.sendTransform(_+1)
	def dec() = state.sendTransform(_-1)
	def current = state.current
}

object CounterState {
	def run(n: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val counter = new CounterState()
		var limit = n
		while(limit > 0) {
			counter.inc()
			limit -= 1
		}
		counter.current
	}

	def runWithBackpressure(n: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val counter = new CounterState()
		def loop(i:Int): Future[Int] = {
			if (i == 0) {
				counter.current
			} else {
				counter.inc().flatMap { case () => loop(i-1) }
			}
		}
		loop(n)
	}
}

class PingPongActor()(implicit ec: ExecutionContext) {
	var peer: PingPongActor = null
	val state = SequentialState[List[Int]](v = Nil)
	def setPeer(newPeer: PingPongActor) {
		peer = newPeer
	}
	def ping(n: Int): Future[Unit] = {
		state.sendTransform(n :: _).flatMap { case () =>
			if (n == 1) {
				Future.successful(())
			} else {
				peer.ping(n-1)
			}
		}
	}

	def get:Future[List[Int]] = state.current
}

object PingPongActor {
	def run(n: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val a = new PingPongActor()
		val b = new PingPongActor()
		a.setPeer(b)
		b.setPeer(a)
		a.ping(n).flatMap { case () =>
			a.get.zip(b.get).map { case (a,b) => a.size + b.size }
		}
	}
}


class PipelineStage[T,R](
	bufLen: Int,
	process: Function[T,Future[R]],
	handleCompleted: Function[List[Try[R]], Future[_]]
)(implicit ec: ExecutionContext) {
	val state = SequentialState(v = new State())

	class State {
		val queue = new mutable.Queue[Future[R]]()
		var outgoing: Option[Future[_]] = None
	}

	private def _drainCompleted(state: State): Unit = {
		// called any time a future completes, either an item in `queue` or `outgoing`
		if (state.outgoing.isDefined) {
			return
		}

		// pop off completed items, but only in order (i.e. stop at the first incomplete item)
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
						fut.onComplete(_ => this.state.awaitAccess { state =>
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

	private def drainCompleted():Future[Unit] = state.sendAccess(_drainCompleted)

	def enqueue(item: T):Future[Unit] = {
		state.awaitAccess { state =>
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
		}.flatMap(identity)
	}
}

object Pipeline {
	def run(
		stages: Int,
		len:Int,
		bufLen: Int,
		parallelism: Int,
		timePerStep: Float,
		jitter: Float
	)(implicit ec: ExecutionContext): Future[Int] = {
		val threadPool = Executors.newFixedThreadPool(parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		val source = Iterator.continually { 0 }.take(len)
		val sink = SequentialState(0)
		val drained = Promise[Int]()
		def finalize(batch: List[Try[Option[Int]]]): Future[Unit] = {
			val transformSent = sink.sendTransform { current =>
				val addition = batch.map(_.get.getOrElse(0)).sum
				current + addition
				// println(s"after batch $addition ($batch), size = ${state.get}")
			}

			if (batch.exists(_.get.isEmpty)) {
				// read the final state
				transformSent.flatMap { case () =>
					sink.awaitAccess { count =>
						drained.success(count)
					}
				}
			} else {
				transformSent
			}
		}

		def process(item: Option[Int]):Future[Option[Int]] = {
			Future({
				item.map { (item: Int) =>
					Sleep.jittered(timePerStep, jitter)
					item + 1
				}
			})(workEc)
		}

		def connect(
			sink: Function[List[Try[Option[Int]]], Future[Unit]],
			stages: Int):Function[List[Try[Option[Int]]], Future[Unit]] =
		{
			val stage = new PipelineStage(bufLen, process, sink)
			def handleBatch(batch: List[Try[Option[Int]]]): Future[Unit] = {
				batch.foldLeft(Future.successful(())) { (acc, item) =>
					acc.flatMap { case () =>
						stage.enqueue(item.get)
					}
				}
			}
			if (stages == 1) {
				handleBatch
			} else {
				connect(handleBatch, stages - 1)
			}
		}

		val fullPipeline = connect(finalize, stages)
		def pushWork():Future[Unit] = {
			if (source.hasNext) {
				val item = source.next
				fullPipeline(List(Success(Some(item)))).flatMap { case () => pushWork() }
			} else {
				fullPipeline(List(Success(None)))
			}
		}

		pushWork().flatMap { case () =>
			drained.future.onComplete( _ => threadPool.shutdown())
			drained.future
		}
	}

	def runMonix(
		stages: Int,
		len: Int,
		parallelism: Int,
		timePerStep: Float,
		bufLen: Int,
		jitter: Float
	)(implicit sched: monix.execution.Scheduler):Future[Int] = {

		val threadPool = Executors.newFixedThreadPool(parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		import monix.eval._
		import monix.reactive._
		import monix.reactive.OverflowStrategy.BackPressure

		val source = Observable.fromIterator( Iterator.continually { 0 }.take(len) )

		def process(item: Int):Future[Int] = {
			Future({
				Sleep.jittered(timePerStep, jitter)
				item + 1
			})(workEc)
		}

		def connect(obs: Observable[Int], stages: Int): Observable[Int] = {
			if (stages == 0) return obs
			connect(obs, stages - 1).map(process)
				.asyncBoundary(OverflowStrategy.BackPressure(bufLen))
				.mapFuture(identity)
		}

		val pipeline = connect(source, stages)
		val sink = pipeline.consumeWith(Consumer.foldLeft(0)(_ + _))

		val ret = sink.runAsync
		ret.onComplete { (_:Try[Int]) => threadPool.shutdown() }
		ret
	}

	def runAkka(
		stages: Int,
		len: Int,
		parallelism: Int,
		timePerStep: Float,
		bufLen: Int,
		jitter: Float
	)(implicit system: ActorSystem, materializer: Materializer):Future[Int] = {
		import akka.stream._
		import akka.stream.scaladsl._
		import akka.{ NotUsed, Done }
		import akka.util.ByteString
		import scala.concurrent._
		import scala.concurrent.duration._
		import java.nio.file.Paths

		val threadPool = Executors.newFixedThreadPool(parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		// val promise = Promise[Int]()
		val source: Source[Option[Int], NotUsed] = Source.fromIterator(() =>
			Iterator.continually { Some(0) }.take(len)
		)
		val sink = Sink.fold[Int,Option[Int]](0) { (i, token) =>
			// println(s"after batch $token, size = ${i}")
			i + token.getOrElse(0)
		}

		def process(item: Option[Int]):Future[Option[Int]] = {
			Future({
				item.map { (item:Int) =>
					Sleep.jittered(timePerStep, jitter)
					item + 1
				}
			})(workEc)
		}

		def connect(
			source: Source[Option[Int], NotUsed],
			stages: Int): Source[Option[Int], NotUsed] = {
			if (stages == 0) {
				source
			} else {
				connect(source.mapAsync(bufLen)(process), stages - 1)
			}
		}

		val flow = connect(source, stages)

		flow.toMat(sink)(Keep.right).run().map({ ret =>
			threadPool.shutdown()
			ret
		})(workEc)
	}

}

object ActorExample {
	val threadPool = Executors.newFixedThreadPool(4)
	implicit val ec = ExecutionContext.fromExecutor(threadPool)
	// val globalEc = scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def simpleCounter(limit: Int): Future[Int] = {
		val lineCount = new SampleCountingActor()
		def loop(i: Int): Future[Int] = {
			lineCount.inc().flatMap { _: Unit =>
				val nextI = i+1
				if (nextI == limit) {
					lineCount.current
				} else {
					loop(nextI)
				}
			}
		}
		loop(0)
	}

	def countWithSequentialStates(lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleActorWordCount()
		val lineCount = new SampleCountingActor()
		def loop(): Future[(Int, Int)] = {
			if (lines.hasNext) {
				for {
					_: Unit <- wordCounter.feed(lines.next())
					_: Unit <- lineCount.inc()
					// XXX potentially no need to wait until the last round?
					// result <- numWords.zip(numLines).flatMap(_ => loop())
					result <- loop()
				} yield result
			} else {
//				println("final line!")
				for {
					words <- wordCounter.current
					lines <- lineCount.current
				} yield (words, lines)
			}
		}

		loop()
	}

	def time(impl: () => Future[_]):(Int,_) = {
		val start = System.currentTimeMillis()
		// println("Start")
		val f = impl
		// println("(computed)")
		val result = Try {
			Await.result(f(), Duration(3, TimeUnit.SECONDS))
		}
		if (result.isFailure) {
			println(result)
		}
		val end = System.currentTimeMillis()
		val duration = end - start
		// println("Done")
		(duration.toInt, result)
	}

	def repeat(n: Int)(name: String, impls: List[(String, ()=> Future[_])]) {
		val averages = impls.map { case (name, fn) =>
      var attempt = n
      var accum:Int = n + 1
      val results = mutable.Set[Any]()
      while(attempt>0) {
        val (cost,result) = time(fn)
        results.add(result)
        if (attempt <= n) {
          accum += cost
        }
        attempt -= 1
        Thread.sleep(10)
      }
      val avg = accum.toFloat / (n-1).toFloat
			(name, avg, results)
		}

		println(s"\nComparison: $name")
		averages.foreach { case (name, avg, results) =>
			val result: Any = if (results.size == 1) results.head else "[MULTIPLE ANSWERS] " + results
			println(s"  ${avg.toInt}ms (average): $name computed $result")
		}
	}

	def run(): Unit = {
		val repeat = this.repeat(12) _
		val bufLen = 10

		// // count lines (2 actors in parallel)
		// repeat {
		// 	val numLines = 3000
		// 	time("SequentialState: word count", countWithSequentialStates(bufLen = bufLen, lines=makeLines(numLines)))
		// }
    //
		// // simple counter (1 actor in serial)
		// repeat {
		// 	val countLimit = 10000
		// 	time("SequentialState: simple counter", simpleCounter(bufLen = bufLen, limit=countLimit))
		// }
    //
		// repeat {
		// 	val countLimit = 1000000
		// 	time("while loop counter", {
		// 		var i = 0
		// 		while (i < countLimit) {
		// 			i += 1
		// 		}
		// 		Future.successful(i)
		// 	})
		// }
    //
		// repeat {
		// 	time("SequentialState: explosive ping", PingPongActor.run(1000, bufLen = bufLen))
		// }

		// repeat {
		// 	time("SequentialState: n=5, t=0, x100 pipeline", Pipeline.run(
		// 		stages = 5,
		// 		len = 100,
		// 		bufLen = bufLen,
		// 		parallelism = 4,
		// 		timePerStep = 0,
		// 		jitter = 0.5f
		// 	))
		// }
		//
		
		import akka.actor.ActorSystem
		implicit val actorSystem = ActorSystem("akka-example")
		implicit val akkaMaterializer = ActorMaterializer()

		val countLimit = 100000
		repeat("counter", List(
			"akka counter" -> (() => CounterActor.run(countLimit)),
//			"seq counter" -> (() => CounterState.run(countLimit)),
			"seq-backpressure counter" -> (() => CounterState.runWithBackpressure(countLimit))
		))

		// pipeline comparison:
		val stages = 6
		val len = 2000
		val parallelism = 6
		val timePerStep = 0f
		val jitter = 0.3f
		val monixScheduler = monix.execution.Scheduler(ec)

		repeat("pipeline", List(
      s"Akka: n=$parallelism, t=$timePerStep, x$len pipeline" -> (() => Pipeline.runAkka(
        stages = stages,
        len = len,
        bufLen = bufLen,
        parallelism = parallelism,
        timePerStep = timePerStep,
        jitter = jitter
      )),

      s"SequentialState: n=$parallelism, t=$timePerStep, x$len pipeline" -> (() => Pipeline.run(
        stages = stages,
        len = len,
        bufLen = bufLen,
        parallelism = parallelism,
        timePerStep = timePerStep,
        jitter = jitter
      )),

      s"Monix: n=$parallelism, t=$timePerStep, x$len pipeline" -> (() => Pipeline.runMonix(
        stages = stages,
        len = len,
        bufLen = bufLen,
        parallelism = parallelism,
        timePerStep = timePerStep,
        jitter = jitter
      )(monixScheduler))
		))

		println("Done - shutting down...")
		threadPool.shutdown()
		actorSystem.terminate()
	}
}

object PerfRun {
	def main(): Unit = {
		val threadPool = Executors.newFixedThreadPool(4)
		implicit val ec = ExecutionContext.fromExecutor(threadPool)
		Await.result(CounterState.run(Int.MaxValue), Duration.Inf)
	}
}


object ActTest {
	def main():Unit = {
		ActorExample.run
//    PerfRun.main()
	}
}
