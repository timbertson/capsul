package net.gfxmonk.capsul.perf
import net.gfxmonk.capsul._
import net.gfxmonk.capsul.mini
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ForkJoinPool, TimeUnit}
import java.util.concurrent.locks.LockSupport

import internal.Log

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}

import scala.annotation.tailrec

object Sleep {
	def jittered(base: Float, jitter: Float) = {
		val jitterMs = (Random.nextFloat() * base * jitter)
		val sleepMs = Math.max(0.0, (base + jitterMs))
		LockSupport.parkNanos((sleepMs * 1000).toInt)
	}
}

object Stats {
	def mean(xs: List[Int]): Double = xs match {
		case Nil => 0.0
		case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
	}

	def median(xs: List[Int]): Int = xs match {
		case Nil => 0
		case ys => ys.sorted.apply(ys.size/2)
	}

	def stddev(xs: List[Int]): Double = xs match {
		case Nil => 0.0
		case ys => {
			val avg = Stats.mean(xs)
			math.sqrt((0.0 /: ys) { (a,e) =>
				a + math.pow(e - avg, 2.0)
			} / xs.size)
		}
	}
}

class SampleWordCountState(bufLen: Int)(implicit sched: ExecutionContext) {
	val state = Capsul(0, bufLen = bufLen)
	def feed(line: String) = state.sendTransform(_ + line.split("\\w").length)
	def reset() = state.sendSet(0)
	def current = state.current
}

class SampleCountingState(bufLen: Int)(implicit ec: ExecutionContext) {
	val state = Capsul(v = 0, bufLen = bufLen)
	def inc() = state.sendTransform(_ + 1)
	def current = state.current
}

class CounterActor extends Actor {
	import CounterActor._

	var i = 0

	def receive = {
		case Increment => i += 1
		case Decrement => i -= 1
		case GetCount => sender ! i
	}
}

class CounterActorWithFeedback extends Actor {
	import CounterActor._

	var i = 0

	def receive = {
		case Increment => { i += 1; sender ! ()}
		case Decrement => { i -= 1; sender ! ()}
		case GetCount => sender ! i
	}
}

object BackpressureActor {
	case object WorkCompleted
}

class BackpressureActor(bufLen: Int, impl: ActorRef)(implicit timeout: Timeout) extends Actor {
	import BackpressureActor._
	private var numInProgress = 0
	private var queue = mutable.Queue[(Any,ActorRef)]()

	private def submitOperation(operation: Any, sender: ActorRef) {
		implicit val ec: ExecutionContext = context.dispatcher
		numInProgress += 1
		val result = (impl ? operation)
		sender ! result // work submitted, you can do more stuff now
		result.onComplete(_ => self ! WorkCompleted)
	}

	def receive = {
		case WorkCompleted => {
			numInProgress -= 1
			while (numInProgress < bufLen && queue.nonEmpty) {
				val (operation, sender) = queue.dequeue()
				submitOperation(operation, sender)
			}
		}
		case operation => {
			if (numInProgress == bufLen) {
				queue.enqueue((operation, sender))
			} else {
				submitOperation(operation, sender)
			}
		}
	}
}

object CounterActor {
	sealed trait Message
	case object Increment extends Message
	case object Decrement extends Message
	case object GetCount extends Message

	def run(n: Int)(implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Int] = {
		import akka.actor.Props
		val counter = system.actorOf(Props[CounterActor])
		var limit = n
		while(limit > 0) {
			counter ! Increment
			limit -= 1
		}
		(counter ? GetCount).mapTo[Int]
	}

	def runWithBackpressure(n: Int, bufLen: Int)(implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout): Future[Int] = {
		import akka.actor.Props
		val counterImpl = system.actorOf(Props[CounterActorWithFeedback])
		val actor = system.actorOf(Props(new BackpressureActor(bufLen, counterImpl)))
		def loop(i:Int): Future[Int] = {
			if (i == 0) {
				(actor ? GetCount).mapTo[Future[Int]].flatMap(identity)
			} else {
				(actor ? Increment).mapTo[Future[Unit]].flatMap { case _: Future[Unit] =>
					// work submitted; continue!
					loop(i-1)
				}
			}
		}
		loop(n)
	}
}

class CounterState(thread: CapsulExecutor)(implicit ec: ExecutionContext) {
	private val state = Capsul(0, thread)
	def inc() = state.sendTransform { current =>
		Log(s"incrementing $current -> ${current+1}")
		current+1
	}
	def dec() = state.sendTransform(_-1)
	def current = state.current
}

object CounterState {
	def run(n: Int, thread: CapsulExecutor)(implicit ec: ExecutionContext): Future[Int] = {
		val counter = new CounterState(thread)
		var limit = n
		while(limit > 0) {
			counter.inc()
			limit -= 1
		}
		counter.current
	}

	def runWithBackpressure(n: Int, thread: CapsulExecutor)(implicit ec: ExecutionContext): Future[Int] = {
		import Log.log
		val counter = new CounterState(thread)
		def loop(i:Int): Future[Int] = {
			val logId = Log.scope(s"runWithBackpressure($i)")
			if (i == 0) {
				log("EOF")
				counter.current
			} else {
				val p = counter.inc()
				log(s"inc: $i")
				p.flatMap { case () => {
					loop(i-1)
				}}
			}
		}
		loop(n)
	}
}

object SimpleCounterState {
	def run(n: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val counter = mini.Capsul(0)
		var limit = n
		while(limit > 0) {
			counter.transform(_+1)
			limit -= 1
		}
		counter.current
	}
}


case class PipelineConfig(
	stages: Int,
	len: Int,
	bufLen: Int,
	parallelism: Int,
	timePerStep: Float,
	jitter: Float) {
	def expectedResult =
		Iterable.range(0, len).foldLeft(0) {
			_ + _ + stages
		}
}

class Pipeline(conf: PipelineConfig)(implicit ec: ExecutionContext) {
	import Log.log
	val threadPool = PerfTest.makeThreadPool(conf.parallelism)
	val workEc = ExecutionContext.fromExecutor(threadPool, e => {
		println("executor.reportException")
		throw e
	})
	def add(item: Int) = {
		Sleep.jittered(conf.timePerStep, conf.jitter)
		item + 1
	}
	def sourceIterator() = Iterable.range(0, conf.len).toIterator

	def process(item: Int):Future[Int] = {
		Future(add(item))(workEc)
	}

	def runSeq2(makeThread: Int => CapsulExecutor)(implicit ec: ExecutionContext): Future[Int] = {
		import Pipeline._
		val source = sourceIterator()
		val sink = Capsul(0, makeThread(conf.bufLen))

		val drained = Promise[Int]()
		def finalize(item: Option[Int]): Future[Unit] = {
			val logId = Log.scope(s"finalize($item)")
			log(s"finalize for item: $item")
			item match {
				case Some(item) => sink.sendTransform { current =>
					val logId = Log.scope(s"finalize")
					val sum = current + item
					log(s"size = ${sum} after adding $item to $current")
					sum
				}
				case None => {
					// read the final state
					log("reading the final state")
					sink.sendAccess[Unit] { count =>
						log(s"final count: $count")
						if (count != conf.expectedResult) {
							drained.failure(new RuntimeException(s"incorrect result: ${count}"))
						} else {
							drained.success(count)
						}
					}
				}
			}
		}

		def singleStage(sink: Function[Option[Int], Future[Unit]], depth: Int):Function[Option[Int], Future[Unit]] = {
			val queue = new ConcurrentLinkedQueue[Item]()
			val pushing: Option[Option[Future[Unit]]] = None
			val pushExecutor = makeThread(conf.bufLen)
			val popState = mini.Capsul(true)

			def markUnblocked(_enqueued: Try[Unit]): Unit = {
				// override canPush to true and complete from head
				popState.sendTransform { _: Boolean =>
					completeFromHead(true)
				}
			}

			@tailrec
			def completeFromHead(canPush: Boolean): Boolean = {
				if (!canPush) return false
				val logId = Log.scope(s"completeFromHead[$depth]")
				Option(queue.peek()) match {
					case Some((calc, promise)) => calc match {
						case Some(f) => f.value match {
							case None => {
								log(s"head is incomplete; continuing")
								true
							}
							case Some(result) => {
								log(s"head is complete, removing it")
								queue.remove()

								val downstream = sink(Some(result.get))
								downstream.onComplete(promise.complete)
								val accepted = downstream.isCompleted
								if (accepted) {
									completeFromHead(true)
								} else {
									// prevent further downstream pushes
									downstream.onComplete(markUnblocked)
									false
								}
							}
						}
						case None => {
							log("saw None")
							queue.remove()
							sink(None)
							true // shouldn't be any more work, so whatever...
						}
					}
					case None => true
				}
			}

			item => {
				val logId = Log.scope(s"handle[$depth]")
				log(s"received item: $item")
				pushExecutor.enqueueOnly(StagedWork.EnqueueOnlyAsync[Unit] { () =>
					val logId = Log.scope(s"calculation[$depth]")
					log(s"calculating from input: $item")
					val calc = item.map(process)
					val done = Promise[Unit]()
					queue.add((calc, done))
					calc.getOrElse(Future.unit).onComplete { _ =>
						popState.sendTransform(completeFromHead)
					}
					done.future
				})
			}
		}

		def connect(
			sink: Function[Option[Int], Future[Unit]],
			stages: Int):Function[Option[Int], Future[Unit]] =
		{
			if (stages == 0) {
				sink
			} else {
				singleStage(connect(sink, stages-1), stages)
			}
		}

		val fullPipeline = connect(finalize, conf.stages)
		def pushWork(): Unit = {
			val logId = Log.scope(s"Pipeline")
			if (source.hasNext) {
				val item = source.next
				log(s"pushWork: begin item $item")
				fullPipeline(Some(item)).onComplete {
					case Success(()) => pushWork()
					case Failure(e) => drained.failure(e)
				}
			} else {
				log(s"pushWork: ending pipeline")
				fullPipeline(None)
			}
		}

		pushWork()
		drained.future.onComplete { result =>
			Log(s"Shutting down ${threadPool} after result: ${result}}")
			threadPool.shutdown()
		}
		drained.future
	}

	def runMonix()(implicit sched: monix.execution.Scheduler):Future[Int] = {
		import monix.eval._
		import monix.reactive._
		import monix.reactive.OverflowStrategy.BackPressure

		val source = Observable.fromIterator(sourceIterator())

		def connect(obs: Observable[Int], stages: Int): Observable[Int] = {
			if (stages == 0) return obs
			connect(obs, stages - 1).map(process)
				.asyncBoundary(OverflowStrategy.BackPressure(conf.bufLen))
				.mapFuture(identity)
		}

		val pipeline = connect(source, conf.stages)
		val sink = pipeline.consumeWith(Consumer.foldLeft(0)(_ + _))

		val ret = sink.runAsync
		ret.onComplete { (_:Try[Int]) => threadPool.shutdown() }
		ret
	}

	def runAkkaStreams()(implicit system: ActorSystem, materializer: Materializer):Future[Int] = {
		import akka.stream._
		import akka.stream.scaladsl._
		import akka.{ NotUsed, Done }
		import scala.concurrent._

		val source: Source[Int, NotUsed] = Source.fromIterator(sourceIterator)
		val sink = Sink.fold[Int,Int](0) { (i, token) =>
			val logId = Log.scope(s"runAkkaStreams")
			log(s"accumulating $i + $token")
			i + token
		}

		def process(item: Int):Future[Int] = {
			val logId = Log.scope(s"runAkkaStreams")
			log(s"begin processing item $item")
			Future(add(item))(workEc)
		}

		def connect(
			source: Source[Int, NotUsed],
			stages: Int): Source[Int, NotUsed] = {
			if (stages == 0) {
				source
			} else {
				connect(source.mapAsync(conf.bufLen)(process), stages - 1)
			}
		}

		val flow = connect(source, conf.stages)

		flow.toMat(sink)(Keep.right).run().map({ ret =>
			threadPool.shutdown()
			// throw new RuntimeException(s"I got $ret")
			ret
		})(workEc)
	}

}
object Pipeline {
	type Item = (Option[Future[Int]], Promise[Unit])
}

object PerfTest {
	def main(): Unit = {
		new PerfTest().main()
	}

	def makeThreadPool(parallelism: Int) = {
		// Executors.newFixedThreadPool(parallelism)
		new ForkJoinPool(parallelism)
	}
}

class PerfTest {
	val bufLen = 50

	val threadPool = PerfTest.makeThreadPool(4)
	implicit val ec = ExecutionContext.fromExecutor(threadPool, e => {
		println("executor.reportException")
		throw e
	})
	// val globalEc = scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def simpleCounter(limit: Int): Future[Int] = {
		val lineCount = new SampleCountingState(bufLen)
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

	def countWithCapsul(lines: Iterator[String]): Future[(Int,Int)] = {
		val wordCounter = new SampleWordCountState(bufLen)
		val lineCount = new SampleCountingState(bufLen)
		def loop(): Future[(Int, Int)] = {
			if (lines.hasNext) {
				for {
					_: Unit <- wordCounter.feed(lines.next())
					_: Unit <- lineCount.inc()
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

	def time(desc: String, impl: () => Future[_]):(Int,_) = {
		val start = System.currentTimeMillis()
		val f = impl
		val result = Try {
			// Await.result(f(), Duration(2, TimeUnit.SECONDS))
			Await.result(f(), Duration(6, TimeUnit.SECONDS))
			// Await.result(f(), Duration.Inf)
		}
		if (result.isFailure) {
			Log("** failed **")
			Log.dump()
			println(s"$desc: $result")
			System.exit(1)
			throw new RuntimeException("failed") // uncomment for early-exit
		}
		val end = System.currentTimeMillis()
		val duration = end - start
		(duration.toInt, result)
	}

	def repeat(n: Int, warmups:Int=1)(name: String, impls: List[(String, ()=> Future[_])]) {
		val runStats = impls.map { case (name, fn) =>
			var attempt = n+warmups
			val times = new mutable.Queue[Int]()
			val results = mutable.Set[Any]()
			while(attempt>0) {
				Log.clear()
				val desc=s"$name#$attempt"
				Log(s"$desc: begin")
				val (cost,result) = time(desc, fn)
				Log(s"$desc: end")
				results.add(result)
				if (attempt <= n) {
					times.enqueue(cost)
				}
				attempt -= 1
				Thread.sleep(10)
			}
			(name, Stats.median(times.toList), Stats.mean(times.toList).toInt, (times.toList.sorted), results)
		}

		println(s"\nComparison: $name")
		runStats.foreach { case (name, median, avg, runs, results) =>
			val result: Any = if (results.size == 1) results.head else "[MULTIPLE ANSWERS] " + results
			println(s"	${avg}ms mean, ${median}ms median, runs=${runs.toSet.toList.sorted}: $name computed $result")
		}
	}

	def main(): Unit = {
		val repeat = this.repeat(200, warmups=50) _
//		val repeat = this.repeat(10, warmups=0) _
//		val repeat = this.repeat(1, warmups=0) _
		val countLimit = 10000
		val largePipeline = PipelineConfig(
			stages = 10,
			len = 1000,
			bufLen = bufLen,
			parallelism = 6,
			timePerStep = 0.1f,
			jitter = 0.3f
		)

		import akka.actor.ActorSystem
		implicit val actorSystem = ActorSystem("akka-example", defaultExecutionContext=Some(ec))
		implicit val akkaMaterializer = ActorMaterializer()
		val monixScheduler = monix.execution.Scheduler(ec)

		// so yuck! Note that each actor's context has a receiveTimeout
		// but it might be infinite, so we can't use it?
		implicit val timeout: Timeout = Timeout(1.minute)

		repeat("counter", List(
			"SimpleCapsul (unbounded) counter" -> (() => SimpleCounterState.run(countLimit)),
			"Capsul / SequentialExecutor (unbounded) counter" -> (() => CounterState.run(countLimit, SequentialExecutor(bufLen))),
			"Capsul / SequentialExecutor (backpressure) counter" -> (() => CounterState.runWithBackpressure(countLimit, SequentialExecutor(bufLen))),
			"Capsul / BackpressureExecutor (unbounded) counter" -> (() => CounterState.run(countLimit, BackpressureExecutor(bufLen))),
			"Capsul / BackpressureExecutor (backpressure) counter" -> (() => CounterState.runWithBackpressure(countLimit, BackpressureExecutor(bufLen))),
			"Akka counter" -> (() => CounterActor.run(countLimit)),
			"Akka counter (backpressure)" -> (() => CounterActor.runWithBackpressure(countLimit, bufLen = bufLen))
		))

		def runPipelineComparison(desc: String, conf: PipelineConfig) = {
			repeat(s"$desc pipeline ($conf, ${conf.expectedResult})", List(
				s"* Capsul (BpEx)" -> (() => new Pipeline(conf).runSeq2(c => BackpressureExecutor(c))),
//				s"* Capsul (SeqEx)" -> (() => new Pipeline(conf).runSeq2(c => SequentialExecutor(c))),
				s"Akka Streams" -> (() => new Pipeline(conf).runAkkaStreams())
				// s"Monix" -> (() => new Pipeline(conf).runMonix()(monixScheduler))
			))
		}

		runPipelineComparison("tiny", largePipeline.copy(len=20, stages=2))
		runPipelineComparison("shallow + short", largePipeline.copy(len=largePipeline.len/10, stages=largePipeline.stages/3))
		runPipelineComparison("shallow + medium", largePipeline.copy(len=largePipeline.len/2, stages=largePipeline.stages/3))
		runPipelineComparison("shallow + long", largePipeline.copy(stages=largePipeline.stages/3))
		runPipelineComparison("medium", largePipeline.copy(stages=largePipeline.stages/2))
		runPipelineComparison("large", largePipeline)
		runPipelineComparison("deep", largePipeline.copy(stages=largePipeline.stages*2, parallelism=2))

		println("Done - shutting down...")
		Await.result(actorSystem.terminate(), 2.seconds)
		println("ActorSystem terminated...")
		threadPool.shutdown()
		threadPool.awaitTermination(2, TimeUnit.SECONDS)
		println("ThreadPool terminated...")
	}
}

object LongLivedLoop {
	def main(): Unit = {
		// val threadPool = PerfTest.makeThreadPool(4)
		// implicit val ec = ExecutionContext.fromExecutor(threadPool)
		// Await.result(CounterState.run(Int.MaxValue, bufLen = PerfTest.bufLen), Duration.Inf)
		while(true) {
			PerfTest.main()
		}
	}
}

