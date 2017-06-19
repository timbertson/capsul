// import monix.execution.Scheduler.Implicits.global
import monix.execution.misc.{AsyncSemaphore,AsyncQueue}
import monix.execution.FutureUtils
import monix.eval._
import monix.reactive._
import monix.reactive.OverflowStrategy.BackPressure
import scala.concurrent.duration._
// import scala.concurrent._
import scala.collection.mutable._
import scala.util.Random
import scala.util.{Success,Failure}
import java.util.concurrent.atomic.AtomicInteger

import monix.execution.Ack.{Stop, Continue}
import monix.execution.cancelables.CompositeCancelable
import monix.execution.{Ack, Cancelable, CancelableFuture}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import scala.concurrent.{Future, Promise}

class DrainFuturesImmediately[A](obs: Observable[Future[A]]) extends Observable[A] {
	def unsafeSubscribeFn(out: Subscriber[A]): Cancelable = {
		import out.scheduler
		obs.unsafeSubscribeFn(new Subscriber[Future[A]] {
			implicit val scheduler = out.scheduler

			// mutable state:
			val lock:Object = this // just need an object to synchronize on
			var isStopped = false
			val unconsumedItem = MVar.empty[A]

			val noopTask = Task.now(())
			val noopFuture = CancelableFuture.successful(())

			// start the emit loop, which emits one item at a time
			// to the subscriber
			val emitLoopThread:CancelableFuture[Unit] = {
				def loop(): CancelableFuture[Unit] = {
					unconsumedItem.take.runAsync.flatMap { item =>
						if (lock.synchronized { isStopped }) {
							noopFuture
						} else {
							out.onNext(item).flatMap {
								case Continue => loop()
								case Stop => {
									stop()
									noopFuture
								}
							}
						}
					}
				}
				loop()
			}

			private def stop():Boolean = {
				lock.synchronized {
					val didStop = !isStopped
					if (didStop) {
						isStopped = true
						emitLoopThread.cancel()
					}
					didStop
				}
			}

			def onNext(elem: Future[A]): Future[Ack] = lock.synchronized {
				if (isStopped) return Future.successful(Stop)
				FutureUtils.materialize(elem).flatMap {
					case Success(x) => unconsumedItem.put(x).map(_ => Continue).runAsync
					case Failure(error) => {
						if (stop()) {
							out.onError(error)
						}
						return Future.successful(Stop)
					}
				}
			}

			def onComplete():Unit = {
				if (stop()) {
					out.onComplete()
				}
			}

			def onError(e: Throwable):Unit = lock.synchronized {
				if (stop()) {
					out.onError(e)
				}
			}
		})
	}
}
