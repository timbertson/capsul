package net.gfxmonk.sequentialstate

import scala.util._
import scala.concurrent._

object StagedFuture {
	def successful[A](value: A): StagedFuture[A] = Future.successful(Future.successful(value))
}
