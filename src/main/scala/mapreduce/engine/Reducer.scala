package mapreduce.engine

import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes.Mappings

object Reducer {
  def apply[K, V, R](reducing: (K, Seq[V]) => KeyValue[K, R])(chunk: Mappings[K, V]): Map[K, R] = {
    chunk map { case (key, values) => val result = reducing(key, values)
      result.key -> result.value
    }
  }
}
