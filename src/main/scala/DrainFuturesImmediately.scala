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
import monix.reactive.observers.{Subscriber,SafeSubscriber}
import scala.concurrent.{Future, Promise}

class DrainFuturesImmediately[A](obs: Observable[Future[A]]) extends Observable[A] {
	def unsafeSubscribeFn(_out: Subscriber[A]): Cancelable = {
		val out = SafeSubscriber(_out)
		import out.scheduler
		obs.unsafeSubscribeFn(new Subscriber[Future[A]] {
			implicit val scheduler = out.scheduler

			def onNext(elem: Future[A]): Future[Ack] = {
				FutureUtils.materialize(elem).flatMap {
					case Success(x) => out.onNext(x)
					case Failure(error) => {
						out.onError(error)
						return Future.successful(Stop)
					}
				}
			}

			def onComplete() = out.onComplete()
			def onError(e: Throwable) = out.onError(e)
		})
	}
}
