package net.gfxmonk.capsul

import java.util.concurrent.atomic._
import java.util.concurrent.{Executors, TimeUnit, ForkJoinPool}
import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.JavaConversions._

import org.openjdk.jcstress.annotations.Expect._
import org.openjdk.jcstress.annotations._
import org.openjdk.jcstress.infra.results._

@JCStressTest
@Outcome(id = Array("50"), expect = ACCEPTABLE, desc = "100 - 50")
@State
class AdditionStressTest {
	import testsupport.Common._
	implicit val ec = defaultEc
	val state = Capsul(0, bufLen)

	@Actor
	def actor1() {
		repeat(100) { state.sendTransform(_ + 1) }
	}

	@Actor
	def actor2() {
		repeat(50) { state.sendTransform(_ - 1) }
	}

	@Arbiter
	def arbiter(r: I_Result) {
		r.r1 = await(state.current)
	}
}
