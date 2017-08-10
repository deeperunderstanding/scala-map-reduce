package mapreduce.examples

import mapreduce.api.MapReduce
import mapreduce.util.{EngineByName, FileToLines}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object WordCountApp extends App {

  args match {
    case Array(filename) => run(filename)

    case Array(filename, engine) => run(filename, engine)

    case arguments =>
      println(s"ERROR: Wrong amount of arguments supplied: ${arguments.length}")
      println("Application takes 1 or 2 parameters:")
      println("[filename] [engine]")
      println("options for engine are: [multi] [actor]")

  }

  def run(filename: String, engine: String = "single"): Unit = {

    val lines = FileToLines(filename)
    val startTime = System.currentTimeMillis()
    val result = MapReduce(WordCount)(EngineByName(engine))(lines.get) //TODO .get

    val printResult = result.map { engineResult =>
      println(s"completed in ${System.currentTimeMillis() - startTime} ms")
      engineResult.result.toVector.sortBy(-_._2).foreach { case (key, value) =>
        println(s"$key - $value")
      }
    }

    Await.result(printResult, Duration.Inf)
  }
}




