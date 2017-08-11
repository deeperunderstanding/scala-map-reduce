package mapreduce.util

import akka.japi.JavaPartialFunction.NoMatch
import mapreduce.api.MapReduceEngine
import mapreduce.engine.actors.MultiThreadedWithActor
import mapreduce.engine.futures.MultiThreadedWithFutures
import mapreduce.engine.single.SingleThreaded

object EngineByName {
  def apply(name: String): MapReduceEngine =
    name match {
      case "multi" => MultiThreadedWithFutures()
      case "actor" => MultiThreadedWithActor()
      case "single" => SingleThreaded
      case other => println(s"ERROR: no engine found with name: $other"); throw NoMatch
    }
}
