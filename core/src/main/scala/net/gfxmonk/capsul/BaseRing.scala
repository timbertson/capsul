package net.gfxmonk.capsul

private [capsul] object BaseRing {
	type Count = Int
	type Idx = Int
}

private [capsul] class BaseRing[T >: Null <: AnyRef](size: Int) {
	import BaseRing._
	private val bounds = size * 2
	private val negativeOne = bounds - 1
	private var arr = Array.fill(size)(new RingItem[T])

	final def mask(idx: Idx): Idx = (idx % size)

	final def at(idx: Idx): RingItem[T] = arr(mask(idx)) // unsafe woo

	final def add(i: Idx, n: Int) = {
		// assumes `n` is never greater than `bounds`
		val result = i + n
		if (result >= bounds) {
			result - bounds
		} else {
			result
		}
	}

	final def inc(a: Idx) = {
		// assuming this is more efficient than add(a,1)
		if (a == negativeOne) 0 else a + 1
	}

	// actually adding a negative is problematic due to `%` behaviour for negatives
	final def dec(a: Idx) = add(a, negativeOne)

	final def numItems(head: Idx, tail: Idx):Int = {
		// queue is full when tail has wrapped around to one less than head
		val diff = tail - head
		if (diff < 0) {
			diff + bounds // there's never negative space available, wrap around
		} else {
			diff
		}
	}
}

