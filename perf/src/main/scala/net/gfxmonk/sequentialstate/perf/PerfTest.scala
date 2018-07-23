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

import akka.pattern.ask
import akka.util.Timeout
import akka.actor.{ActorSystem,Actor,ActorRef}
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

class SampleWordCountState(bufLen: Int)(implicit sched: ExecutionContext) {
	val state = SequentialState(0, bufLen = bufLen)
	def feed(line: String) = state.sendTransform(_ + line.split("\\w").length)
	def reset() = state.sendSet(0)
	def current = state.current
}

class SampleCountingState(bufLen: Int)(implicit ec: ExecutionContext) {
	val state = SequentialState(v = 0, bufLen = bufLen)
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

class CounterState(bufLen: Int)(implicit ec: ExecutionContext) {
	private val state = SequentialState(0, bufLen = bufLen)
	def inc() = state.sendTransform { current =>
		Log(s"incrementing $current -> ${current+1}")
		current+1
	}
	def dec() = state.sendTransform(_-1)
	def current = state.current
}

object CounterState {
	def run(n: Int, bufLen: Int)(implicit ec: ExecutionContext): Future[Int] = {
		val counter = new CounterState(bufLen)
		var limit = n
		while(limit > 0) {
			counter.inc()
			limit -= 1
		}
		counter.current
	}

	def runWithBackpressure(n: Int, bufLen: Int)(implicit ec: ExecutionContext): Future[Int] = {
		import Log.log
		val counter = new CounterState(bufLen)
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


// XXX investigate why this implementation appears to stall on large pipelines...
// class PipelineStage[T,R](
// 	index: Int,
// 	bufLen: Int,
// 	process: Function[T,Future[R]],
// 	handleCompleted: Function[R, Future[Unit]]
// )(implicit ec: ExecutionContext) {
// 	import Log.log
// 	val stateRef = SequentialState(v = new State(), bufLen = bufLen)
// 	val logId = Log.scope(s"PipelineStage-${index}")
//
// 	class State {
// 		var queue = Queue[Future[R]]()
// 		var pending: Future[Unit] = Future.successful(())
// 	}
//
// 	private def onItemCompleted(item: R): Future[Unit] = {
// 		// need to re-access state
// 		stateRef.accessAsync { state =>
// 			if (!state.pending.isCompleted) {
// 				state.pending.onComplete { _ => onItemCompleted(item) }
// 				return state.pending
// 			}
//
// 			val (readyItems, newQueue) = state.queue.span(_.isCompleted)
// 			state.pending = if (readyItems.isEmpty) {
// 				log(s"item processed: ${item} but nothing ready in ${newQueue}")
// 				Future.successful(())
// 			} else {
// 				log(s"Emitting events downstream: ${readyItems} (on completion of ${item}). pending: ${newQueue}")
// 				val initial = state.pending
// 				val batchPushed = readyItems.foldLeft(initial) { (acc, item) =>
// 					acc.flatMap { case () =>
// 						handleCompleted(item.value.get.get)
// 					}
// 				}
// 				state.queue = newQueue
// 				batchPushed
// 			}
// 			state.pending
// 		}
// 	}
//
// 	def enqueue(item: T):Future[Unit] = {
// 		log(s"enqueueing ${item}")
// 		stateRef.sendAccessAsync { state =>
// 			log(s"manipulating state for ${item}")
// 			val processedItem = process(item)
// 			state.queue = state.queue.enqueue(processedItem)
// 			processedItem.flatMap(onItemCompleted)
// 		}
// 	}
// }

object PipelineStage {
	val completedFuture = Future.successful(())
	class State[R] {
		var queue = Queue[(Future[R], Promise[Unit])]()
		var outgoing: Future[Unit] = completedFuture
		override def toString(): String = s"State($queue, $outgoing)"
	}
}

class PipelineStage[T,R](
	index: Int,
	bufLen: Int,
	process: Function[T,Future[R]],
	handleCompleted: Function[R, Future[Unit]]
)(implicit ec: ExecutionContext) {
	import Log.log
	import PipelineStage._
	val stateRef = SequentialState(v = new State[R](), bufLen = bufLen)
	val logId = Log.scope(s"PipelineStage-${index}")

	private def onItemCompleted(item: Any): Future[Unit] = {
		// need to re-access state
		stateRef.accessAsync { state =>
			if (!state.outgoing.isCompleted) {
				state.outgoing.onComplete { _ => onItemCompleted(item) }
				log(s"outgoing is still running on completion of ${item}, returning")
				return state.outgoing
			}

			// queue up a new batch
			val (readyItems, newQueue) = state.queue.span(_._1.isCompleted)
			state.outgoing = if (readyItems.isEmpty) {
				log(s"item processed: ${item} but nothing ready in ${newQueue}")
				completedFuture
			} else {
				state.queue = newQueue
				log(s"Emitting events downstream: ${readyItems} (on completion of ${item}). incomplete: ${newQueue}")
				val initial = state.outgoing
				val everythingPushed = readyItems.foldLeft(initial) { (acc, item) =>
					acc.flatMap { case () =>
						val emitted = handleCompleted(item._1.value.get.get)
						emitted.onComplete(_ => item._2.success(()))
						emitted
					}
				}
				everythingPushed.onComplete { _ =>
					log(s"batch ${readyItems} is fully pushed out; calling onItemCompleted in case there are more completed items")
					onItemCompleted(())
				}
				everythingPushed
			}
			state.outgoing
		}
	}

	def enqueue(item: T):Future[Unit] = {
		log(s"enqueueing ${item}")
		stateRef.sendAccessAsync { state =>
			log(s"manipulating state for ${item}, state = $state")
			val promise = Promise[Unit]()
			val processedItem = process(item)
			state.queue = state.queue.enqueue((processedItem, promise))
			processedItem.onComplete(onItemCompleted)
			promise.future
		}
	}
}

case class PipelineConfig(stages: Int, len: Int, bufLen: Int, parallelism: Int, timePerStep: Float, jitter: Float)

class Pipeline(conf: PipelineConfig)(implicit ec: ExecutionContext) {
	import Log.log
	val threadPool = PerfTest.makeThreadPool(conf.parallelism)
	val workEc = ExecutionContext.fromExecutor(threadPool)
	val logId = Log.scope(s"Pipeline")
	def add(item: Int) = {
		Sleep.jittered(conf.timePerStep, conf.jitter)
		item + 1
	}
	def sourceIterator() = Iterable.range(0, conf.len).toIterator

	def runSeq()(implicit ec: ExecutionContext): Future[Int] = {
		val source = sourceIterator()
		val sink = SequentialState(0, bufLen = conf.bufLen)
		val drained = Promise[Int]()
		def finalize(item: Option[Int]): Future[Unit] = {
			item match {
				case Some(item) => sink.sendTransform { current =>
					val sum = current + item
					log(s"size = ${sum} after adding $item to $current")
					sum
				}
				case None => {
					// read the final state
					log("reading the final state")
					sink.access { count =>
						log(s"final count: $count")
						// println(s"final count: $count")
						// drained.failure(new RuntimeException("Got: " + count))
						drained.success(count)
					}
				}
			}
		}

		def process(item: Option[Int]):Future[Option[Int]] = {
			log(s"begin processing item $item")
			Future(item.map(add))(workEc)
		}

		def connect(
			sink: Function[Option[Int], Future[Unit]],
			stages: Int):Function[Option[Int], Future[Unit]] =
		{
			val logId = Log.scope(s"connect[$stages]")
			val stage = new PipelineStage(stages, conf.bufLen, process, sink)
			if (stages == 1) {
				stage.enqueue
			} else {
				connect(stage.enqueue, stages - 1)
			}
		}

		val fullPipeline = connect(finalize, conf.stages)
		def pushWork():Future[Unit] = {
			if (source.hasNext) {
				val item = source.next
				log(s"pushWork: begin item $item")
				fullPipeline(Some(item)).flatMap { case () => pushWork() }
			} else {
				log(s"pushWork: ending pipeline")
				fullPipeline(None)
			}
		}

		pushWork().flatMap { case () =>
			drained.future.onComplete( _ => threadPool.shutdown())
			drained.future
		}
	}

	def runMonix()(implicit sched: monix.execution.Scheduler):Future[Int] = {
		import monix.eval._
		import monix.reactive._
		import monix.reactive.OverflowStrategy.BackPressure

		val source = Observable.fromIterator(sourceIterator())

		def process(item: Int):Future[Int] = {
			Future(add(item))(workEc)
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

	def runAkkaStreams()(implicit system: ActorSystem, materializer: Materializer):Future[Int] = {
		import akka.stream._
		import akka.stream.scaladsl._
		import akka.{ NotUsed, Done }
		import scala.concurrent._

		val source: Source[Int, NotUsed] = Source.fromIterator(sourceIterator)
		val sink = Sink.fold[Int,Int](0) { (i, token) =>
			log(s"accumulating $i + $token")
			i + token
		}

		def process(item: Int):Future[Int] = {
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

object PerfTest {
	val bufLen = 50
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

	def countWithSequentialStates(lines: Iterator[String]): Future[(Int,Int)] = {
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

	def time(impl: () => Future[_]):(Int,_) = {
		val start = System.currentTimeMillis()
		val f = impl
		val result = Try {
			Await.result(f(), Duration(3, TimeUnit.SECONDS))
			// Await.result(f(), Duration.Inf)
		}
		if (result.isFailure) {
			Log("** failed **")
			Log.dump()
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
		val repeat = this.repeat(1) _
		val countLimit = 10000
		val largePipeline = PipelineConfig(
			stages = 10,
			len = 1000,
			bufLen = bufLen,
			parallelism = 4,
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


		def runPipelineComparison(desc: String, conf: PipelineConfig) = {
			repeat(s"$desc pipeline ($conf)", List(
				s"* SequentialState" -> (() => new Pipeline(conf).runSeq()),
				s"Akka Streams" -> (() => new Pipeline(conf).runAkkaStreams()),
				s"Monix" -> (() => new Pipeline(conf).runMonix()(monixScheduler))
			))
		}

		repeat("counter", List(
			"SequentialState (unbounded) counter" -> (() => CounterState.run(countLimit, bufLen = bufLen)),
			"SequentialState (backpressure) counter" -> (() => CounterState.runWithBackpressure(countLimit, bufLen = bufLen)),
			"Akka counter" -> (() => CounterActor.run(countLimit)),
			"Akka counter (backpressure)" -> (() => CounterActor.runWithBackpressure(countLimit, bufLen = bufLen))
		))
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
		val threadPool = PerfTest.makeThreadPool(4)
		implicit val ec = ExecutionContext.fromExecutor(threadPool)
		Await.result(CounterState.run(Int.MaxValue, bufLen = PerfTest.bufLen), Duration.Inf)
	}
}

