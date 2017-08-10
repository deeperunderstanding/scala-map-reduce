package mapreduce.engine

import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes.Mappings

object Mapper {
  def apply[I, K, V](mapping: (I) => Seq[KeyValue[K, V]])(chunk: Seq[I]): Mappings[K, V] = {
    Merge.keyValuesToMap(chunk flatMap mapping)
  }
}

