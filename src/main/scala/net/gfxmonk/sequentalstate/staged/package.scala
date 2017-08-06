package net.gfxmonk.sequentialstate

import scala.concurrent._
import scala.util._

package object staged {
	implicit class StagedFutureExt[A](val f: Future[Future[A]]) extends AnyVal {
		def staged(implicit ex: ExecutionContext): Future[Unit] = f.map((_:Future[A]) => ())
		def onStage(fn:Function0[_])(implicit ex: ExecutionContext): Unit = f.onComplete((_:Try[Future[A]]) => ())
		def resolved(implicit ex: ExecutionContext): Future[A] = f.flatMap(identity)
	}
}
