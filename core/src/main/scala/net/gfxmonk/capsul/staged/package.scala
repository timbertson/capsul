package net.gfxmonk.capsul

import scala.concurrent._
import scala.util._

package object staged {
	implicit class StagedFutureExt[T](val f: Future[T]) extends AnyVal {
		// def flatMapAccept[U](fn: Function[T,Future[U]])(implicit ec: ExecutionContext): StagedFuture[U] = {
		// 	StagedFuture(f.map(fn))
		// }

		def flatMapStaged[U](fn: Function[T,StagedFuture[U]])(implicit ec: ExecutionContext): StagedFuture[U] = {
			// TODO: can we do without the wrapping & unwrapping here?
			StagedFuture(f.flatMap(x => fn(x).accepted))
		}
	}
}
