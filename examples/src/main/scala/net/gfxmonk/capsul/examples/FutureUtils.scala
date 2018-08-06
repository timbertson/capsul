package net.gfxmonk.capsul.examples

import scala.concurrent.{ExecutionContext, Future}

object FutureUtils {
  def foldLeft[Accum,Elem](empty: Accum, elems: TraversableOnce[Elem])
    (fn: (Accum, Elem) => Future[Accum])
    (implicit ec: ExecutionContext): Future[Accum] =
  {
    val it = elems.toIterator
    def next(accum: Accum): Future[Accum] = {
      if (it.hasNext) {
        fn(accum, it.next).flatMap(next)
      } else {
        Future.successful(accum)
      }
    }
    next(empty)
  }
}
