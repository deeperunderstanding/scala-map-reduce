package mapreduce.engine.futures

import mapreduce.api.{EngineResult, MapReduce, MapReduceEngine}
import mapreduce.engine._
import Merge._

import scala.concurrent._
import scala.reflect._

class MultiThreadedWithFutures(numberOfWorkers: Int) extends MapReduceEngine {

  override def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag](program: MapReduce[I, K, V, R])(data: Seq[I]): Future[EngineResult[K, R]] = {

    implicit val context = ExecutionContext.global

    val mappingChunks: Seq[Seq[I]] = data.grouped(data.size / numberOfWorkers).toVector

    val futureMappings = FutureMapAndCollect(program.mapper)(mappingChunks) map { mappings => Merge(mappings) }

    val futureReducings = futureMappings flatMap { merged =>
      val reducingChunks = merged.grouped(merged.size / numberOfWorkers).toVector

      FailAllOnException(reducingChunks map { chunk =>
        Future {
          Reducer(program.reducer)(chunk)
        }
      })
    }

    futureReducings map { values => EngineResult(values.merge) }

  }
}

object MultiThreadedWithFutures {
  def apply(numberOfWorkers: Int = 4): MultiThreadedWithFutures = new MultiThreadedWithFutures(numberOfWorkers)
}