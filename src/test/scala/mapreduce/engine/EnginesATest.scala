package mapreduce.engine

import akka.util.Timeout
import mapreduce.TestProgram
import mapreduce.api.{EngineResult, MapReduce, MapReduceEngine}
import mapreduce.engine.actors.MultiThreadedWithActor
import mapreduce.engine.futures.MultiThreadedWithFutures
import mapreduce.engine.single.SingleThreaded
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class EnginesATest extends FlatSpec with Matchers {

  implicit val timeout = Timeout(5.minutes)

  "Executing a map-reduce program in the either version of the engine" should "produce the correct result for " +
    "that program from all engines" in {

    val expectedResult = EngineResult(TestProgram.expectedReducingResult)
    val mockMapReduce = MapReduce(TestProgram)(_ : MapReduceEngine)(TestProgram.testInput)

    val singleThreadedResult = Await.result(mockMapReduce(SingleThreaded), 1.second)
    val multiThreadedWithFuturesResult = Await.result(mockMapReduce(MultiThreadedWithFutures(2)), 1.second)
    val multiThreadedWithActorsResult = Await.result(mockMapReduce(MultiThreadedWithActor(2)), 1.second)

    singleThreadedResult shouldBe expectedResult
    multiThreadedWithFuturesResult shouldBe expectedResult
    multiThreadedWithActorsResult shouldBe expectedResult

  }

}
