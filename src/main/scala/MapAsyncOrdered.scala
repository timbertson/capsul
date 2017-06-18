// import monix.execution.Scheduler.Implicits.global
import monix.execution.misc.AsyncSemaphore
import monix.eval._
import monix.reactive._
import monix.reactive.OverflowStrategy.BackPressure
import scala.concurrent.duration._
// import scala.concurrent._
import scala.collection.mutable._
import scala.util.Random
import scala.util.{Success,Failure}

import monix.execution.Ack.{Stop, Continue}
import monix.execution.cancelables.CompositeCancelable
import monix.execution.{Ack, Cancelable, CancelableFuture}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import scala.concurrent.{Future, Promise}

object MapAsyncOrdered {
	implicit class Implicit[T](obs: Observable[T]) {
		def parallelMapPreservingOrder[R](bufferSize:Int)(fn:T=>Task[R]): Observable[R] = {
			// See https://github.com/monix/monix/issues/329
			new MapAsyncOrdered(obs, bufferSize, fn)
		}
	}
}


class MapAsyncOrdered[A,B](obs: Observable[A], maxBuffer: Int, fn: A => Task[B]) extends Observable[B] {
	def unsafeSubscribeFn(out: Subscriber[B]): Cancelable = {
		import out.scheduler
		obs.unsafeSubscribeFn(new Subscriber[A] {
			implicit val scheduler = out.scheduler

			// mutable state:
			val buffer = ArrayBuffer.empty[CancelableFuture[B]]
			val lock:Object = buffer // we just need an object to synchronize on, might as well be the buffer
			var isStopped = false
			var respondUpstream = noResponseNecessary
			val unconsumedItem = MVar.empty[B]

			val noResponseNecessary:Function[Ack,Unit] = _ => ()
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

			private def stop() {
				lock.synchronized {
					isStopped = true
					emitLoopThread.cancel()
					respondUpstream(Stop)
				}
			}

			val emitCompleted:Task[Unit] = Task {
				lock.synchronized {
					if (isStopped) {
						noopTask
					} else {
						buffer.headOption.flatMap(_.value) match {
							case None => noopTask
							case Some(Success(value)) => {
								buffer.remove(0)
								respondUpstream(Continue)
								// once we've put the item into the outgoing mvar,
								// emit the next ready item (if any)
								unconsumedItem.put(value).flatMap(_ => emitCompleted)
							}
							case Some(Failure(err)) => {
								if (!isStopped) {
									out.onError(err)
									stop()
								}
								buffer.foreach(_.cancel)
								noopTask
							}
						}
					}
				}
			}.flatMap(identity)


			def onNext(elem: A): Future[Ack] = lock.synchronized {
				if (isStopped) return Future.successful(Stop)

				val future = fn(elem).runAsync
				buffer.append(future)

				val returnVal = if (buffer.size < maxBuffer) {
					// buffer still has room
					respondUpstream = noResponseNecessary
					Future.successful(Continue)
				} else {
					// buffer is full, promise will be completed when an item is emitted
					val promise = Promise[Ack]()
					respondUpstream = promise.trySuccess
					promise.future
				}

				// note: we don't enqueue this until `respondUpstream` has been set
				future.transformWith(_ => {
					emitCompleted.runAsync
				})

				returnVal
			}

			def onComplete():Unit = lock.synchronized {
				if (!isStopped) {
					out.onComplete()
				}
			}

			def onError(e: Throwable):Unit = lock.synchronized {
				if (!isStopped) {
					out.onError(e)
				}
			}
		})
	}
}
