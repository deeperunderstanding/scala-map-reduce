package mapreduce.engine.single

import mapreduce.api.{EngineResult, MapReduce, MapReduceEngine}
import mapreduce.engine.{Mapper, Reducer}

import scala.concurrent.Future
import scala.reflect._
import scala.util.Try

object SingleThreaded extends MapReduceEngine {
  override def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag](program: MapReduce[I, K, V, R])(data: Seq[I])
  : Future[EngineResult[K, R]] = Future.fromTry {
    Try {
      EngineResult[K, R](Reducer(program.reducer)(Mapper(program.mapper)(data)))
    }
  }
}
