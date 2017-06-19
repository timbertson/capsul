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

class BufferedRunAsync[A,B](obs: Observable[A], maxBuffer: Int, fn: A => Task[B]) extends Observable[Future[B]] {
	def unsafeSubscribeFn(out: Subscriber[Future[B]]): Cancelable = {
		import out.scheduler
		obs.unsafeSubscribeFn(new Subscriber[A] {
			implicit val scheduler = out.scheduler

			// mutable state:
			val buffer = AsyncQueue.empty[CancelableFuture[B]]
			val lock:Object = buffer // we just need an object to synchronize on, might as well be the buffer
			var isStopped = false
			var respondUpstream = noResponseNecessary
			var availableSlots = maxBuffer - 1

			val noResponseNecessary:Function[Ack,Unit] = _ => ()
			val noopTask = Task.now(())
			val noopFuture = CancelableFuture.successful(())

			val emitLoopThread:Future[Unit] = {
				def loop(): Future[Unit] = {
					buffer.poll().flatMap { item =>
						lock.synchronized {
							if (isStopped) {
								noopFuture
							} else {
								availableSlots += 1
								out.onNext(item).flatMap {
									case Continue => {
										respondUpstream(Continue)
										loop()
									}
									case Stop => {
										stop()
										noopFuture
									}
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
						respondUpstream(Stop)
					}
					didStop
				}
			}

			def onNext(elem: A): Future[Ack] = lock.synchronized {
				if (isStopped) return Future.successful(Stop)
				val future = fn(elem).runAsync
				buffer.offer(future)

				lock.synchronized {
					availableSlots -= 1
					if(availableSlots > 0) {
						// buffer has room
						Future.successful(Continue)
					} else {
						// buffer is full, accept another item when any item is removed
						val promise = Promise[Ack]()
						respondUpstream = promise.trySuccess
						promise.future
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
