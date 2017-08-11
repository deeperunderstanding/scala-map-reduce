package mapreduce.util

import akka.japi.JavaPartialFunction.NoMatch
import akka.util.Timeout
import mapreduce.api.MapReduceEngine
import mapreduce.engine.actors.MultiThreadedWithActor
import mapreduce.engine.futures.MultiThreadedWithFutures
import mapreduce.engine.single.SingleThreaded
import scala.concurrent.duration._

object EngineByName {
  def apply(name: String): MapReduceEngine =
    name match {
      case "multi" => MultiThreadedWithFutures()
      case "actor" => MultiThreadedWithActor()(Timeout(5.minutes))
      case "single" => SingleThreaded
      case other => println(s"ERROR: no engine found with name: $other"); throw NoMatch
    }
}
