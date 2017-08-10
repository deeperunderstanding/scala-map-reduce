package mapreduce.engine

import scala.collection.immutable.Queue

object EngineTypes {

  type Mappings[K, V] = Map[K, Queue[V]]
  def emptyMappings[K, V]: Mappings[K, V] = Map[K, Queue[V]]().empty

}