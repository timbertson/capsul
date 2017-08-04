package net.gfxmonk.sequentialstate
import scala.concurrent._

object Backpressured {
	def successful[A](value: A):Future[Future[A]] = Future.successful(Future.successful(value))
	def failed[A](err: Throwable):Future[Future[A]] = Future.successful(Future.failed(err))
	def result[A](value: Future[Future[A]])(implicit ex: ExecutionContext):Future[A] = value.flatMap(identity)
	def enqueued[A](value: Future[Future[A]])(implicit ex: ExecutionContext):Future[Unit] = {
		val p = Promise[Unit]()
		value.onComplete(_ => p.success(()))
		p.future
	}
}
