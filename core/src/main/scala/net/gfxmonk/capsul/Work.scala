package net.gfxmonk.capsul

import scala.concurrent.{ExecutionContext, Future, Promise}

sealed trait SpawnResult
case object Completed extends SpawnResult
case class Async(future: Future[_]) extends SpawnResult
object SpawnResult {
	def future(f: Future[_]): SpawnResult = {
		if (f.isCompleted) {
			Completed
		} else {
			Async(f)
		}
	}
}

// used for simple capsul
trait AtomicWork {
	def run(): Unit
}

object AtomicWork {
	case class EnqueueOnly[A](fn: Function0[A])
		extends AtomicWork {
		final override def run(): Unit = {
			fn()
		}
	}

	case class Full[A](fn: Function0[A])
		extends AtomicWork with HasResultPromise[A] {
		final override def run(): Unit = {
			resultPromise.success(fn())
		}
	}
}

// used for backpressure-aware capsul
trait StagedWork {
	def spawn(): SpawnResult
	def enqueuedAsync(): Unit
}

trait HasResultPromise[A] {
	val resultPromise: Promise[A] = Promise[A]()
}

object StagedWork {
	trait HasEnqueuePromise[A] {
		var enqueuedPromise = Promise[A]()
	}

	trait HasEnqueueAndResultPromise[A] { self: HasEnqueuePromise[Future[A]] with HasResultPromise[A] =>
		final def enqueuedAsync() {
			enqueuedPromise.success(resultPromise.future)
		}
	}

	trait IsEnqueueOnly { self: HasEnqueuePromise[Unit] =>
		final def enqueuedAsync() {
			enqueuedPromise.success(())
		}
	}

	case class FullAsync[A](fn: Function0[Future[A]])(implicit ec: ExecutionContext)
		extends StagedWork
		with HasEnqueuePromise[Future[A]]
		with HasResultPromise[A]
		with HasEnqueueAndResultPromise[A] {

		override def spawn(): SpawnResult = {
			val f = fn()
			f.onComplete(resultPromise.complete)
			SpawnResult.future(f)
		}
	}

	case class Full[A](fn: Function0[A])
		extends StagedWork
			with HasEnqueuePromise[Future[A]]
			with HasResultPromise[A]
			with HasEnqueueAndResultPromise[A] {

		override def spawn(): SpawnResult = {
			resultPromise.success(fn())
			Completed
		}
	}

	case class EnqueueOnlyAsync[A](fn: Function0[Future[A]])
		(implicit ec: ExecutionContext)
		extends StagedWork with HasEnqueuePromise[Unit] with IsEnqueueOnly {

		override def spawn(): SpawnResult = {
			val f = fn()
			SpawnResult.future(f)
		}
	}

	case class EnqueueOnly[A](fn: Function0[A])
		extends StagedWork with HasEnqueuePromise[Unit] with IsEnqueueOnly {

		override def spawn(): SpawnResult = {
			fn()
			Completed
		}
	}
}
