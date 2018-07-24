package net.gfxmonk.sequentialstate

import org.scalatest._

import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.immutable.Queue
import scala.collection.mutable

class RingSpec extends FunSpec {
	import Ring._

	val size = 10
	val minIndex = 0
	val maxIndex = 19
	val validIndices = (minIndex to maxIndex).toList
	val validSizes = (minIndex to size).toList
	def emptyState(idx: Int = 0) = Ring.make(idx, idx, 0)
	def fullState(idx: Int = 0) = Ring.make(idx, idx, 0)
	def withItems(idx: Int, n: Int) = Ring.make(idx, ring.add(idx, n), 0)

	// we're only testing pure functions on ring, so we can reuse an instance
	val ring = new Ring(size)

	def actsLike[Item,Ret](range:Seq[Item], a: Function[Item,Ret], b: Function[Item,Ret]): Unit = {
		val inputs = range.toList
		assert(inputs.map(a) == inputs.map(b))
	}

	def foreachIndexIt(desc: String)(fn: Function[Idx,Unit]) {
		describe(desc) {
			validIndices.foreach { i =>
				it(s"i=$i") { fn(i) }
			}
		}
	}

	describe("make & repr") {
		it("acts like a simple tuple") {
			val examples = List(
				(0,0,0),
				(1,1,1),
				(1,2,3)
			)
			examples.map { case (a,b,c) =>
				assert(Ring.repr(Ring.make(a,b,c)) == (a,b,c))
			}
		}
	}

	describe("add") {
		it("wraps around at 2*size") {
			assert(ring.add(0, 1) == 1)
			assert(ring.add(5, 5) == 10)
			assert(ring.add(19, 1) == 0)
			assert(ring.add(19, 3) == 2)
			assert(ring.add(10, 10) == 0)
		}
	}

	describe("inc") {
		it("acts like add(*,1)") {
			actsLike[Int,Int](validIndices, ring.inc, ring.add(_,1))
		}
	}

	describe("mask") {
		it("acts like x % size") {
			actsLike[Int,Int](validIndices, ring.mask, _ % size)
			assert(ring.mask(19) == 9)
			assert(ring.mask(10) == 0)
		}
	}

	describe("dec") {
		it("wraps at 2*size") {
			assert(ring.dec(0) == 19)
			assert(ring.dec(10) == 9)
			assert(ring.dec(19) == 18)
		}
	}

	describe("numItems(head, tail)") {
		foreachIndexIt("returns the number of items in the ring") { i =>
			validSizes.foreach { n =>
				val state = withItems(i, n)
				val result = ring.numItems(Ring.headIndex(state), Ring.tailIndex(state))
				assert(result == n, s"- ring $state with size $n")
			}
		}
	}

	describe("spaceAvailable") {
		foreachIndexIt("returns the space available") { i =>
			validSizes.foreach { n =>
				val state = withItems(i, n)
				val result = ring.spaceAvailable(state, 0)
				assert(result == size - n, s"- ring $state with size $n")
			}
		}

		it("returns 0 if there are equal (or more) futures than spaces") {
			assert(ring.spaceAvailable(withItems(0, 5), 5) == 0)
			assert(ring.spaceAvailable(withItems(0, 5), 10) == 0)
		}
	}

	describe("dequeueAndReserve") {
		val state = Ring.make(0, 2, 5)
		val newState = ring.dequeueAndReserve(state, 3, 1)

		it("leaves head unchanged") {
			assert(Ring.headIndex(state) == 0)
			assert(Ring.headIndex(newState) == 0)
		}
		it("extends tail by queued+work") {
			assert(Ring.tailIndex(state) == 2)
			assert(Ring.tailIndex(newState) == 6)
		}
		it("decrements queue by numQueue") {
			assert(Ring.numQueued(state) == 5)
			assert(Ring.numQueued(newState) == 2)
		}
	}

	describe("incrementQueued") {
		it("increments queued value") {
			val state = Ring.make(1, 2, 3)
			assert(Ring.incrementQueued(state) == Ring.make(1,2,4))
		}
	}

	describe("isStopped") {
		foreachIndexIt("is true iff the ring is empty") { i =>
			assert(Ring.isStopped(emptyState(i)) == true)
			validSizes.foreach { n =>
				val expected = n == 0
				assert(Ring.isStopped(withItems(i, n)) == expected)
			}
		}
	}
}
