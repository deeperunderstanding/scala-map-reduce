package mapreduce.engine

import mapreduce.MockProgram
import mapreduce.api.{EngineResult, MapReduce}
import mapreduce.engine.actors.MultiThreadedWithActor
import mapreduce.engine.futures.MultiThreadedWithFutures
import mapreduce.engine.single.SingleThreaded
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class EnginesATest extends FlatSpec with Matchers {

  "Executing a map-reduce program in the either version of the engine" should "produce the correct result for " +
    "that program from all engines" in {

    val singleThrededResult = Await.result(MapReduce(MockProgram)(SingleThreaded)(MockProgram.testInput), 1.second)
    val multiThreadedWithFuturesResult = Await.result(MapReduce(MockProgram)(MultiThreadedWithFutures(1))(MockProgram.testInput), 1.second)
    val multiThreadedWithActorsResult = Await.result(MapReduce(MockProgram)(MultiThreadedWithActor(1))(MockProgram.testInput), 1.second)

    singleThrededResult shouldBe EngineResult(MockProgram.expectedReducingResult)
    multiThreadedWithFuturesResult shouldBe EngineResult(MockProgram.expectedReducingResult)
    multiThreadedWithActorsResult shouldBe EngineResult(MockProgram.expectedReducingResult)

  }

}
