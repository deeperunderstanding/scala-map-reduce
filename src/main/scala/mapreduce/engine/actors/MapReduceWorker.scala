package mapreduce.engine.actors

import akka.actor.Actor
import mapreduce.api.KeyValue
import mapreduce.engine.EngineTypes.Mappings
import mapreduce.engine.actors.Messages.{MappingResult, MappingWork, ReducingResult, ReducingWork}
import mapreduce.engine.{Mapper, Reducer}

import scala.reflect.ClassTag

class MapReduceWorker[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag]
(mapping: (I) => Seq[KeyValue[K, V]], reducing: (K, Seq[V]) => KeyValue[K, R]) extends Actor {

  override def receive: Receive = {
    case MappingWork(chunk) => sender ! MappingResult(Mapper(mapping)(chunk.data.asInstanceOf[Seq[I]]))

    case ReducingWork(chunk) => sender ! ReducingResult(Reducer(reducing)(chunk.asInstanceOf[Mappings[K, V]]))

  }
}
