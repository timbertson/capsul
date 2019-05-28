package net.gfxmonk.capsul

private [capsul] class RingItem[T>:Null<:AnyRef] {
	@volatile private var contents:T = null
	def set(item:T) {
		contents = item
	}

	def get: T = {
		contents
	}
}
