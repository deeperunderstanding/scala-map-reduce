package mapreduce.engine

import mapreduce.engine.futures.FailAllOnException
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class FailOnExceptionUTest extends FlatSpec with Matchers {

  val exception = new Exception("too bad :(")

  "combining a sequence of futures" should "convert them to a future of a sequence of the result type, " +
    "which terminates immediately and results in the occurred exception" in {

    val future1 = Future { Thread.sleep(5000); 1 }
    val future2 = Future { Thread.sleep(5000); 2 }
    val future3 = Future { throw exception }

    val combined = FailAllOnException(Seq(future1, future2, future3))

    val error = intercept[Exception] { Await.result(combined, 1.second) }
    error shouldBe exception

  }

}
