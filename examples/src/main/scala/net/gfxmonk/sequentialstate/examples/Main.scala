package net.gfxmonk.sequentialstate.examples

import net.gfxmonk.sequentialstate.Log

object Main extends App {
	println("== readme")
	readme.Main.main()
	println("== wordcount")
	wordcount.ExampleMain.main()
	println("== async")
	async.ExampleMain.main()
	println("== chain")
	chain.ExampleMain.main()
	println("== perf")
	PerfTest.main()
}