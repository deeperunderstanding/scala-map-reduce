package mapreduce.engine.futures

import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes.Mappings
import mapreduce.engine.Reducer
import mapreduce.engine.Merge._

import scala.concurrent.{ExecutionContext, Future}

object FutureReduceAndMerge {

  def apply[K, V, R](reducing: (K, Seq[V]) => KeyValue[K, R])(chunks: Seq[Mappings[K, V]])(implicit context: ExecutionContext): Future[Map[K, R]] = {
    FailAllOnException(chunks map { chunk =>
      Future {
        Reducer(reducing)(chunk)
      }
    }) map (_.merge)
  }
}
