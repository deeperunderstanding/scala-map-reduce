package mapreduce.engine

import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes._

import scala.collection.immutable.{HashMap, Queue}

object Merge {

  def apply[K, V](mappings: Seq[Mappings[K, V]]): Mappings[K, V] =
    mappings.flatten.foldLeft(HashMap.empty[K, Queue[V]]) { case (aggregate, (key, value)) =>
      aggregate + (key -> aggregate.getOrElse(key, Queue.empty).enqueue(value))
    }

  /**
    * alternative, yet here folding seems to be slightly less efficient
    * {{{
    *   collected.foldLeft(HashMap.empty[K, Queue[V]]) { case (aggregate, KeyValue(key, value)) =>
    *     aggregate + (key -> aggregate.getOrElse(key, Queue.empty).enqueue(value))
    *   }
    * }}}
    */
  def keyValuesToMap[K, V](collected: Seq[KeyValue[K, V]]): Mappings[K, V] = {
    collected.groupBy(_.key) map { case (key, pairs) =>
      key -> Queue[V](pairs map (_.value): _*)
    }
  }

  implicit class MappingsAppend[K, V](accumulated: Mappings[K, V]) {

    def append(other: Mappings[K, V]): Mappings[K, V] = {
      other.foldLeft(accumulated) { case (aggregate, keyVal) => val (key, values) = keyVal
        aggregate + (key -> aggregate.getOrElse(key, Queue.empty).enqueue(values))
      }
    }
  }

  implicit class MergeMaps[K, V](maps: Seq[Map[K, V]]) {
    def merge: Map[K, V] = maps.reduce(_ ++ _)
  }

}
