package mapreduce.engine.futures

import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes.Mappings
import mapreduce.engine.Mapper

import scala.concurrent.{ExecutionContext, Future}

object FutureMapAndCollect {
  def apply[I, K, V]
  (mapping: (I) => Seq[KeyValue[K, V]])(chunks: Seq[Seq[I]])(implicit context: ExecutionContext): Future[Seq[Mappings[K, V]]] =
    FailAllOnException(chunks.map { chunk =>
      Future {
        Mapper(mapping)(chunk)
      }
    })


}


