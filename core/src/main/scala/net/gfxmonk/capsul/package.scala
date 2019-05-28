package net.gfxmonk

import scala.concurrent.Future

/** @see [[Capsul]] */
package object capsul {
  type StagedFuture[T] = Future[Future[T]]
}
