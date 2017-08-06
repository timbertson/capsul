package net.gfxmonk.sequentialstate

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._

trait StagedFuture[T] extends Future[T] {
	def accepted: Future[Unit]
	def onAccept[U](fn: Future[T] => U)(implicit ex: ExecutionContext): Unit
	def asNestedFuture: Future[Future[T]]
}

object StagedFuture {
	final def successful[A](value: A): StagedFuture[A] = new Enqueued(Future.successful(value)) // could be more specialised, but used rarely
	final def accepted[A](future: Future[A]): StagedFuture[A] = new Enqueued(future)
	final def apply[A](future: Future[Future[A]]): StagedFuture[A] = new Wrapped(future)

	private class Enqueued[T](f: Future[T]) extends StagedFuture[T] {
		final def accepted: Future[Unit] = Future.successful(())
		final def onAccept[U](fn: Future[T] => U)(implicit ex: ExecutionContext): Unit = fn(f)
		final def asNestedFuture: Future[Future[T]] = Future.successful(f)

		// scala.concurrent.Awaitable
		final def ready(atMost: Duration)(implicit permit: scala.concurrent.CanAwait): this.type = {
			f.ready(atMost)(permit)
			this
		}
		final def result(atMost: Duration)(implicit permit: CanAwait): T = f.result(atMost)(permit)

		// scala.concurrent.Future
		final def isCompleted: Boolean = f.isCompleted
		final def onComplete[U](fn: Try[T] => U)(implicit executor: ExecutionContext): Unit = f.onComplete(fn)
		final def transform[S](fn: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] = f.transform(fn)
		final def transformWith[S](fn: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] = f.transformWith(fn)
		final def value: Option[Try[T]] = f.value
	}

	private class Wrapped[T](f: Future[Future[T]]) extends StagedFuture[T] {
		// StagedFuture extra interface
		final def accepted: Future[Unit] = new DiscardingFuture(f)
		final def onAccept[U](fn: Future[T] => U)(implicit ex: ExecutionContext): Unit = {
			f.onComplete {
				case Success(f) => fn(f)
				case Failure(e) => fn(Future.failed(e)) // extremely uncommon
			}
		}

		final def asNestedFuture: Future[Future[T]] = f

		// scala.concurrent.Awaitable
		def ready(atMost: Duration)(implicit permit: scala.concurrent.CanAwait): this.type = {
			f.ready(atMost)(permit)
			f.value match {
				case None => this
				case Some(Success(inner)) => {
					// XXX need to subtract the already-waited time from `atMost`
					inner.ready(atMost)(permit)
					this
				}
				case Some(Failure(_)) => this
			}
		}

		def result(atMost: Duration)(implicit permit: CanAwait): T = {
			f.ready(atMost)(permit)
			// XXX need to subtract the already-waited time from `atMost`
			f.value.get.get.result(atMost)(permit)
		}

		// scala.concurrent.Future
		def isCompleted: Boolean = f.value match {
			case None => false
			case Some(Success(inner)) => inner.isCompleted
			case Some(Failure(_)) => true
		}

		def onComplete[U](fn: Try[T] => U)(implicit executor: ExecutionContext): Unit = {
			f.onComplete {
				case Success(inner) => inner.onComplete(fn)
				case Failure(e) => fn(Failure(e))
			}
		}

		def transform[S](fn: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] = {
			f.flatMap(inner => inner.transform(fn))
		}

		def transformWith[S](fn: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] = {
			f.flatMap(inner => inner.transformWith(fn))
		}

		def value: Option[Try[T]] = f.value match {
			case None => None
			case Some(Success(next)) => next.value
			case Some(Failure(e)) => Some(Failure(e))
		}
	}

	private class DiscardingFuture[T](f: Future[T]) extends Future[Unit] {
		def ready(atMost: Duration)(implicit permit: scala.concurrent.CanAwait): this.type = {
			f.ready(atMost)(permit)
			this
		}

		def result(atMost: Duration)(implicit permit: scala.concurrent.CanAwait): Unit = {
			f.ready(atMost)(permit)
			()
		}

		// Members declared in scala.concurrent.Future
		def isCompleted: Boolean = f.isCompleted
		def onComplete[U](fn: Try[Unit] => U)(implicit executor: ExecutionContext): Unit = {
			f.onComplete(result => fn(discardTry(result)))
		}

		private def discardTry[U](t: Try[U]) : Try[Unit] = t match {
			case Success(_) => Success(())
			case Failure(e) => Failure(e)
		}

		def transform[S](fn: Try[Unit] => Try[S])(implicit executor: ExecutionContext): Future[S] = {
			f.transform(x => fn(discardTry(x)))
		}

		def transformWith[S](fn: Try[Unit] => Future[S])(implicit executor: ExecutionContext): Future[S] = {
			f.transformWith(x => fn(discardTry(x)))
		}

		def value: Option[Try[Unit]] = f.value.map(discardTry)
	}
}
