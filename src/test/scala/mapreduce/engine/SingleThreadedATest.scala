package mapreduce.engine

import mapreduce.MockProgram
import mapreduce.api.{EngineResult, MapReduce}
import mapreduce.engine.single.SingleThreaded
import org.scalatest.{AsyncFlatSpec, Matchers}

class SingleThreadedATest extends AsyncFlatSpec with Matchers{

  "Executing a map-reduce program in the single threaded version of the engine" should "produce the correct result for " +
    "that program with the specified types" in {

      MapReduce(MockProgram)(SingleThreaded)(MockProgram.testInput) map {
        result => result shouldBe EngineResult(MockProgram.expectedReducingResult)
      }

  }

}
