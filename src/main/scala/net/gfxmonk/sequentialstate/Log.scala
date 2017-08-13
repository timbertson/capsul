package net.gfxmonk.sequentialstate

import scala.util.Sorting
import scala.collection.mutable

// efficient in-memory threadsafe log collection, used
// for debugging issues (SequentialExecutor is augmented
// with commented-out log calls)

private [sequentialstate] class Log(id: String, buf: Log.LogBuffer) {
	val prefix = s"[$id]: "
	def apply(s: String) {
		Log(buf, prefix+s)
	}
}

private [sequentialstate] class NoopLog() {
	def apply(s: String) = ()
}

private [sequentialstate] object Log {
	type LogEntry = (Long,String)
	type ThreadLogEntry = (Long,LogEntry)
	type LogBuffer = mutable.Queue[LogEntry]

	@volatile private var enabled = false
	def enable() = {
		enabled = true
	}

	lazy val threads = {
		if (!enabled) {
			throw new RuntimeException(
				"Log module has not been enabled; " +
				"you may have accidentally left in a call")
		}
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

	def id(desc: String) = {
		nextId += 1
		new Log(s"$desc.$nextId", threadBuffer)
	}
	def clear() {
		threads.synchronized {
			threads.clear()
		}
	}

	def apply(s: String) {
		apply(threadBuffer, s)
	}

	def apply(buf: LogBuffer, s: String) {
		val time = System.nanoTime()
		buf.enqueue(time -> s)
		if (buf.length > 1000) {
			buf.dequeue()
		}
	}

	def test(s: String) = if (enabled) apply(s)
	def testId(desc: String) = if (enabled) id(desc) else new NoopLog()

	def dump(n: Int) {
		if(!enabled) return
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
	}
}
