package net.gfxmonk.sequentialstate.internal

import scala.util.Sorting
import scala.collection.mutable
import scala.language.experimental.macros

// efficient in-memory threadsafe log collection, used
// for debugging issues (SequentialExecutor is augmented
// with log calls which have zero cost when disabled)

private [sequentialstate] class LogCtx(id: String, val buf: Log.LogBuffer) {
	val prefix = s"[$id]: "
}

object Log {
	// val ENABLE = true; type Ctx = LogCtx
	val ENABLE = false; type Ctx = Unit

	type LogEntry = (Long,String)
	type ThreadLogEntry = (Long,LogEntry)
	type LogBuffer = mutable.Queue[LogEntry]

	private lazy val threads = {
		if (!ENABLE) {
			throw new RuntimeException(
				"Log module has not been enabled")
		}
		println("\n\n ** WARNING ** Log is enabled; this should only be used for debugging\n\n")
		Thread.sleep(500)
		mutable.Map[Long, LogBuffer]()
	}

	def threadBuffer = {
		val id = Thread.currentThread().getId()
		threads.get(id) match {
			case None => {
				threads.synchronized {
					threads.get(id) match {
						case None => {
							val buf: LogBuffer = mutable.Queue[LogEntry]()
							threads.update(id, buf)
							buf
						}
						case Some(buf) => buf
					}
				}
			}
			case Some(buf) => buf
		}
	}

	var nextId: Int = 0

	def scope(desc: String): Ctx = macro Macros.scopeImpl1
	def scope(obj: Any, desc: String): Ctx = macro Macros.scopeImpl2
	def log(msg: String) = macro Macros.logImpl

	private def ifEnabled(x: => Unit) {
		if (ENABLE) x
	}

	def clear() {
		ifEnabled {
			threads.synchronized {
				threads.clear()
			}
		}
	}

	def apply(msg: String): Unit = macro Macros.applyImpl

	def dump(n: Int) {
		ifEnabled {
			// XXX this is racey... just continue until it works?
			while(true) {
				try {
					threads.synchronized {
						val buffers = threads.map { case (tid,logs) =>
							logs.map (log => (tid,log))
						}
						val merged = buffers.foldLeft(Array[ThreadLogEntry]()) { (acc, lst) =>
							// sort by timestamp
							Sorting.stableSort(acc ++ lst, { (a:ThreadLogEntry, b: ThreadLogEntry) =>
								(a,b) match {
									case ((_tida, (tsa, _msga)), (_tidb, (tsb, _msgb))) => {
										tsa < tsb
									}
								}
							})
						}
						println(merged.reverse.take(n).reverse.map { case (time,(tid, msg)) =>
							s"-- $time|$tid: $msg"
						}.mkString("\n"))
					}
					return
				} catch {
					case _:Throwable => ()
				}
			}
		}
	}

	object Macros {
		import scala.reflect.macros.blackbox

		def doIfEnabled(c:blackbox.Context)(expr: c.Expr[Unit]):c.Expr[Unit] = {
			import c.universe._
			if (Log.ENABLE) expr else reify { }
		}

		def returnIfEnabled(c:blackbox.Context)(expr: c.Expr[Log.Ctx]):c.Expr[Log.Ctx] = {
			import c.universe._
			if (Log.ENABLE) expr else reify { ().asInstanceOf[Log.Ctx] }
		}

		def applyImpl(c:blackbox.Context)(msg:c.Expr[String]):c.Expr[Unit] = {
			import c.universe._
			doIfEnabled(c) { c.Expr(q"Log.Macros.logMsg($msg)") }
		}

		def scopeImpl1(c:blackbox.Context)(desc:c.Expr[String]):c.Expr[Log.Ctx] = {
			import c.universe._
			returnIfEnabled(c) { c.Expr(q"Log.Macros.makeId($desc)") }
		}

		def scopeImpl2(c:blackbox.Context)(obj:c.Expr[Any], desc:c.Expr[String]):c.Expr[Log.Ctx] = {
			import c.universe._
			returnIfEnabled(c) { c.Expr(q"Log.Macros.makeId($obj, $desc)") }
		}

		def logImpl(c:blackbox.Context)(msg:c.Expr[String]):c.Expr[Unit] = {
			import c.universe._
			// Can only be called with a `__logCtx` in scope
			doIfEnabled(c) { c.Expr(q"Log.Macros.logWithCtx(logId, $msg)") }
		}

		def logMsg(s: String) {
			// anonymous, global log
			doLog(threadBuffer, s)
		}

		def logWithCtx(ctx: LogCtx, s: String) {
			// thread-local tagged log
			doLog(ctx.buf, ctx.prefix+s)
		}

		def doLog(buf: LogBuffer, s: String) {
			val time = System.nanoTime()
			buf.enqueue(time -> s)
			if (buf.length > 1000) {
				buf.dequeue()
			}
		}

		def makeId(obj: Any, desc: String) = {
			nextId += 1
			val id = if (obj == null) nextId else s"${System.identityHashCode(obj)}.$nextId"
			new LogCtx(s"$desc@$id", Log.threadBuffer)
		}

		def makeId(desc: String) = {
			nextId += 1
			new LogCtx(s"$desc.$nextId", Log.threadBuffer)
		}

	}
}
