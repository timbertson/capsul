package net.gfxmonk.sequentialstate.perf
import net.gfxmonk.sequentialstate._

import monix.eval.Task
import monix.execution.atomic.{Atomic, AtomicAny}
import monix.execution.misc.NonFatal
import java.util.concurrent.{Executors, TimeUnit, ForkJoinPool}
import java.util.concurrent.locks.LockSupport
import internal.Log

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent._
import scala.util._

import akka.actor.{ActorSystem,Actor}
import akka.stream.{Materializer,ActorMaterializer}

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

class SampleWordCountState(implicit sched: ExecutionContext) {
	val state = SequentialState(0)
	def feed(line: String) = state.sendTransform(_ + line.split("\\w").length)
	def reset() = state.sendSet(0)
	def current = state.current
}

class SampleCountingState()(implicit ec: ExecutionContext) {
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
	def inc() = state.sendTransform { current =>
		Log(s"incrementing $current -> ${current+1}")
		current+1
	}
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
		import Log.log
		val counter = new CounterState()
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


class PipelineStage[T,R](
	bufLen: Int,
	process: Function[T,Future[R]],
	handleCompleted: Function[List[Try[R]], Future[_]]
)(implicit ec: ExecutionContext) {
	import Log.log
	val state = SequentialState(v = new State())
	val logId = Log.scope(s"PipelineStage")

	class State {
		val queue = new mutable.Queue[Future[R]]()
		var outgoing: Option[Future[_]] = None
	}

	private def _drainCompleted(state: State): Unit = {
		// called any time a future completes, either an item in `queue` or `outgoing`
		if (state.outgoing.isDefined) {
			log(s"drainCompleted: outgoing already defined")
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
					log(s"drainCompleted: popping $ret")
					if (ret.nonEmpty) {
						// we have some items to produce
						val fut = handleCompleted(ret.toList)
						log(s"state.outgoing = Some($fut)")
						state.outgoing = Some(fut)
						fut.onComplete(_ => this.state.access { state =>
							log(s"state.outgoing = None")
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
		state.access { state =>
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

case class PipelineConfig(stages: Int, len: Int, bufLen: Int, parallelism: Int, timePerStep: Float, jitter: Float)
object Pipeline {
	import Log.log
	def run(conf: PipelineConfig)(implicit ec: ExecutionContext): Future[Int] = {
		val threadPool = PerfTest.makeThreadPool(conf.parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		val source = Iterable.range(0, conf.len).toIterator
		val sink = SequentialState(0)
		val drained = Promise[Int]()
		val logId = Log.scope("Pipeline")
		def finalize(batch: List[Try[Option[Int]]]): Future[Unit] = {
			log(s"sinking ${batch.length} items")
			val transformSent = sink.sendTransform { current =>
				val addition = batch.map(_.get.getOrElse(0)).sum
				log(s"after batch $addition ($batch), size = ${current + addition}")
				current + addition
			}

			if (batch.exists(_.get.isEmpty)) {
				// read the final state
				log("reading the final state")
				transformSent.flatMap { case () =>
					sink.access { count =>
						log(s"final count: $count")
						drained.success(count)
					}
				}
			} else {
				transformSent
			}
		}

		def process(item: Option[Int]):Future[Option[Int]] = {
			log(s"begin processing item $item")
			Future({
				item.map { (item: Int) =>
					Sleep.jittered(conf.timePerStep, conf.jitter)
					item + 1
				}
			})(workEc)
		}

		def connect(
			sink: Function[List[Try[Option[Int]]], Future[Unit]],
			stages: Int):Function[List[Try[Option[Int]]], Future[Unit]] =
		{
			val logId = Log.scope(s"connect[$stages]")
			val stage = new PipelineStage(conf.bufLen, process, sink)
			def handleBatch(batch: List[Try[Option[Int]]]): Future[Unit] = {
				batch.foldLeft(Future.successful(())) { (acc, item) =>
					acc.flatMap { case () =>
						log(s"enqueueing ${item.get}")
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

		val fullPipeline = connect(finalize, conf.stages)
		def pushWork():Future[Unit] = {
			if (source.hasNext) {
				val item = source.next
				Log(s"begin item $item")
				fullPipeline(List(Success(Some(item)))).flatMap { case () => pushWork() }
			} else {
				Log(s"pipeline complete")
				fullPipeline(List(Success(None)))
			}
		}

		pushWork().flatMap { case () =>
			drained.future.onComplete( _ => threadPool.shutdown())
			drained.future
		}
	}

	def runMonix(conf: PipelineConfig)(implicit sched: monix.execution.Scheduler):Future[Int] = {

		val threadPool = PerfTest.makeThreadPool(conf.parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		import monix.eval._
		import monix.reactive._
		import monix.reactive.OverflowStrategy.BackPressure

		val source = Observable.fromIterator( Iterator.continually { 0 }.take(conf.len) )

		def process(item: Int):Future[Int] = {
			Future({
				Sleep.jittered(conf.timePerStep, conf.jitter)
				item + 1
			})(workEc)
		}

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

	def runAkka(
		conf: PipelineConfig
	)(implicit system: ActorSystem, materializer: Materializer):Future[Int] = {
		import akka.stream._
		import akka.stream.scaladsl._
		import akka.{ NotUsed, Done }
		import scala.concurrent._

		val threadPool = PerfTest.makeThreadPool(conf.parallelism)
		val workEc = ExecutionContext.fromExecutor(threadPool)

		val source: Source[Option[Int], NotUsed] = Source.fromIterator(() =>
			Iterator.continually { Some(0) }.take(conf.len)
		)
		val sink = Sink.fold[Int,Option[Int]](0) { (i, token) =>
			i + token.getOrElse(0)
		}

		def process(item: Option[Int]):Future[Option[Int]] = {
			Future({
				item.map { (item:Int) =>
					Sleep.jittered(conf.timePerStep, conf.jitter)
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
				connect(source.mapAsync(conf.bufLen)(process), stages - 1)
			}
		}

		val flow = connect(source, conf.stages)

		flow.toMat(sink)(Keep.right).run().map({ ret =>
			threadPool.shutdown()
			ret
		})(workEc)
	}

}

object PerfTest {
	def makeThreadPool(parallelism: Int) = {
		// Executors.newFixedThreadPool(parallelism)
		new ForkJoinPool(parallelism)
	}

	val threadPool = makeThreadPool(4)
	implicit val ec = ExecutionContext.fromExecutor(threadPool)
	// val globalEc = scala.concurrent.ExecutionContext.Implicits.global
	def makeLines(n:Int=500) = Iterator.continually {
		"hello this is an excellent line!"
	}.take(n)

	def simpleCounter(limit: Int): Future[Int] = {
		val lineCount = new SampleCountingState()
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
		val wordCounter = new SampleWordCountState()
		val lineCount = new SampleCountingState()
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

	def time(impl: () => Future[_]):(Int,_) = {
		val start = System.currentTimeMillis()
		val f = impl
		val result = Try {
			Await.result(f(), Duration(6, TimeUnit.SECONDS))
			// Await.result(f(), Duration.Inf)
		}
		if (result.isFailure) {
			Log("** failed **")
			Log.dump(400)
			println(result)
		}
		val end = System.currentTimeMillis()
		val duration = end - start
		(duration.toInt, result)
	}

	def repeat(n: Int)(name: String, impls: List[(String, ()=> Future[_])]) {
		val runStats = impls.map { case (name, fn) =>
			var attempt = n
			val times = new mutable.Queue[Int]()
			val results = mutable.Set[Any]()
			while(attempt>0) {
				Log.clear()
				Log(s"$name: begin attempt $attempt")
				val (cost,result) = time(fn)
				Log(s"$name: end attempt $attempt")
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
		val repeat = this.repeat(20) _
		val bufLen = 10

		import akka.actor.ActorSystem
		implicit val actorSystem = ActorSystem("akka-example", defaultExecutionContext=Some(ec))
		implicit val akkaMaterializer = ActorMaterializer()

		val countLimit = 10000
		repeat("counter", List(
			"seq-unbounded counter" -> (() => CounterState.run(countLimit)),
			"seq-backpressure counter" -> (() => CounterState.runWithBackpressure(countLimit)),
			"akka counter" -> (() => CounterActor.run(countLimit))
		))

		// pipeline comparison:
		val monixScheduler = monix.execution.Scheduler(ec)

		val largePipeline = PipelineConfig(
			stages = 10,
			len = 1000,
			bufLen = bufLen,
			parallelism = 4,
			timePerStep = 0.1f,
			jitter = 0.3f
		)

		def runPipelineComparison(desc: String, conf: PipelineConfig) = {
			repeat(s"$desc pipeline ($conf)", List(
				s"* SequentialState" -> (() => Pipeline.run(conf)),
				s"Akka" -> (() => Pipeline.runAkka(conf))
				// s"Monix" -> (() => Pipeline.runMonix(conf)(monixScheduler))
			))
		}

		runPipelineComparison("shallow + short", largePipeline.copy(len=100, stages=3))
		runPipelineComparison("shallow + medium", largePipeline.copy(len=500, stages=3))
		runPipelineComparison("shallow + long", largePipeline.copy(stages=3))
		runPipelineComparison("medium", largePipeline.copy(stages=5))
		runPipelineComparison("large", largePipeline)
		runPipelineComparison("deep", largePipeline.copy(stages=20, parallelism=2))

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
		val threadPool = PerfTest.makeThreadPool(4)
		implicit val ec = ExecutionContext.fromExecutor(threadPool)
		Await.result(CounterState.run(Int.MaxValue), Duration.Inf)
	}
}

