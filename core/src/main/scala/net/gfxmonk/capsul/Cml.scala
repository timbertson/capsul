package net.gfxmonk.capsul.cml

import java.util.concurrent.ConcurrentLinkedQueue
import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import net.gfxmonk.capsul.internal.Log
import net.gfxmonk.capsul.EnqueueableTask
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import scala.concurrent.{ExecutionContext, Future}

object Op {
	val WAITING: Int = 0
	val CLAIMED: Int = 1
	val DONE: Int = 2
	type State = AtomicInteger

	def apply[T](op: Op[T]): Future[T] = {
		val promise = Promise[T]()
		if (!op.attempt(promise)) {
			val state = new AtomicInteger(WAITING)
			op.perform(state, promise)
		}
		promise.future
	}

	def choose[T](ops: Op[T]*): Op[T] = {
		return new ChooseOp(ops)
	}
}

class ChooseOp[T](ops: Seq[Op[T]]) extends Op[T] {
	def attempt(promise: Promise[T]): Boolean = {
		ops.foreach { op =>
			if (op.attempt(promise)) {
				return true
			}
		}
		false
	}

	def perform(state: Op.State, promise: Promise[T]): Unit = {
		// try _all_ operations, only the first will actually go ahead
		// note: this may leave "done" states around, which could create garbage. If we had
		// `op.abandon()`, that could clean up state (but might not be cheap)
		ops.foreach(_.perform(state, promise))
	}
}

class WrapOp[T,R](op: Op[T], fn: T => R)(implicit ec: ExecutionContext) extends Op[R] {
	private def proxy(promise: Promise[R]): Promise[T] = {
		val initial = Promise[T]()
		initial.future.foreach(x => promise.success(fn(x)))
		initial
	}

	def attempt(promise: Promise[R]): Boolean = {
		op.attempt(proxy(promise))
	}

	def perform(state: Op.State, promise: Promise[R]): Unit = {
		op.perform(state, proxy(promise))
	}
}

trait Op[T] {
	import Op._
	def attempt(promise: Promise[T]): Boolean
	def perform(state: State, promise: Promise[T]): Unit
	def wrap[R](fn: T => R)(implicit ec: ExecutionContext): Op[R] = {
		new WrapOp(this, fn)
	}
}

class Channel[T] {
	import Channel._
	val senders = new ConcurrentLinkedQueue[Sender[T]]()
	val receivers = new ConcurrentLinkedQueue[Receiver[T]]()
	val recv = new Recv(this)
	def send(value:T) = new Send(this, value)
}

object Channel {
	class Sender[T](val state: Op.State, val value: T, val promise: Promise[Unit])

	class Receiver[T](val state: Op.State, val promise: Promise[T])

	def apply[T]() = new Channel[T]()

	class Recv[T](channel: Channel[T]) extends Op[T] {
		import Op._
		@tailrec final def attempt(promise: Promise[T]): Boolean = {
			channel.senders.peek() match {
				case null => false
				case sender => {
					if (sender.state.compareAndSet(WAITING, DONE)) {
						val removed = channel.senders.peek()
						assert(removed == sender)
						sender.promise.success(())
						promise.success(sender.value)
						true
					} else {
						val state = sender.state.get()
						if (state == DONE) {
							channel.senders.remove(sender)
							// keep trying
							attempt(promise)
						} else {
							false
						}
					}
				}
			}
		}

		def perform(state: State, promise: Promise[T]): Unit = {
			channel.receivers.add(new Receiver(state, promise))

			// now check if work has popped up since attempt()

			val it = channel.senders.iterator()
			@tailrec def loop() {
				if (it.hasNext()) {
					val sender = it.next()
					if (sender.state == state) {
						// that's me!
						loop()
					} else {
						// claim recv
						if (state.compareAndSet(WAITING, CLAIMED)) {
							// claim sender
							if (sender.state.compareAndSet(WAITING, DONE)) {
								// we completed the op!
								state.set(DONE)
								channel.senders.remove(sender)
								sender.promise.success(())
								promise.success(sender.value)
							} else {
								if (sender.state.get() != DONE) {
									// it wasn't done, must be either waiting or claimed Try again
									loop()
								}
							}
						} else {
							// must be `DONE`, since only current thread uses CLAIMED. Yay
						}
					}
				}
			}
			loop()
		}
	}

	class Send[T](channel: Channel[T], value: T) extends Op[Unit] {
		import Op._
		@tailrec final def attempt(promise: Promise[Unit]): Boolean = {
			channel.receivers.peek() match {
				case null => false
				case receiver => {
					if (receiver.state.compareAndSet(WAITING, DONE)) {
						val removed = channel.receivers.poll()
						assert(removed == receiver)
						receiver.promise.success(value)
						promise.success(())
						true
					} else {
						val state = receiver.state.get()
						if (state == DONE) {
							channel.receivers.remove(receiver)
							// keep trying
							attempt(promise)
						} else {
							false
						}
					}
				}
			}
		}

		def perform(state: State, promise: Promise[Unit]): Unit = {
			channel.senders.add(new Sender(state, value, promise))

			// now check if work has popped up since attempt()

			val it = channel.receivers.iterator()
			@tailrec def loop() {
				if (it.hasNext()) {
					val receiver = it.next()
					if (receiver.state == state) {
						// that's me!
						loop()
					} else {
						// claim send
						if (state.compareAndSet(WAITING, CLAIMED)) {
							// claim receiver
							if (receiver.state.compareAndSet(WAITING, DONE)) {
								// we completed the op!
								state.set(DONE)
								channel.receivers.remove(receiver)
								receiver.promise.success(value)
								promise.success(())
							} else {
								if (receiver.state.get() != DONE) {
									// it wasn't done, must be either waiting or claimed Try again
									loop()
								}
							}
						} else {
							// must be `DONE`, since only current thread uses CLAIMED. Yay
						}
					}
				}
			}
			loop()
		}
	}
}


object Main {
	import scala.concurrent.ExecutionContext.Implicits.global

	def main(args: Array[String]) {
		val channel = Channel[String]()

		Op(channel.send("hello")).foreach { item =>
			println("Item sent!")
		}

		Op(channel.recv).foreach { item =>
			println("Item received: " + item)
		}

		println("---")

		val a = Channel[String]()
		val b = Channel[String]()
		val c = Channel[String]()

		val recvOne = Op.choose(a.recv, b.recv, c.recv)
		def loop() {
			Op(recvOne).foreach { item =>
				println("got one! " + item)
				loop()
			}
		}
		loop()

		Await.result(Future {
			Thread.sleep(200)
			Op.apply(a.send("a1"))
			Op.apply(a.send("a2"))

			Op(Op.choose(
				b.send("b3").wrap { case () => println("sent on b, not a") },
				a.send("a3").wrap { case () => println("sent on a, not b") }
			))
			Op(b.send("b1"))
			Op(c.send("c1"))
			println("-----")
		}, Duration.Inf)
	}
}
