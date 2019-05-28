package net.gfxmonk.capsul

import scala.concurrent.{ExecutionContext, Future}

object StagedFuture {
	def successful[T](result: T) = Future.successful(Future.successful(result))
	def failed(result: Throwable) = Future.successful(Future.failed(result))
	def accepted[T](result: Future[T]) = Future.successful(result)

	class StagedExtensions[T](val f: Future[Future[T]]) extends AnyVal {
		def accepted(implicit ec: ExecutionContext): Future[Unit] = f.map(_ => ())
	}

	object Implicits {
		implicit def stagedExtensions[T](f: Future[Future[T]]): StagedExtensions[T] =
			new StagedExtensions[T](f)
	}
}
