package mapreduce.engine.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.RoundRobinPool
import mapreduce.api.{EngineResult, MapReduce}
import mapreduce.engine.EngineTypes.{Mappings, emptyMappings}
import mapreduce.engine.Merge._
import mapreduce.engine.actors.Messages._

import scala.reflect.ClassTag

class MapReduceExecuter[In: ClassTag, Key: ClassTag, Value: ClassTag, Reduced: ClassTag]
(program: MapReduce[In, Key, Value, Reduced])(numberOfWorkers: Int, MaxChunkSize: Int = 10000)
  extends Actor {

  private val mapReduceWorkRouter = context.actorOf(
    Props(new MapReduceWorker[In, Key, Value, Reduced](program.mapper, program.reducer))
      .withRouter(RoundRobinPool(numberOfWorkers)), name = "mapReduceWorkRouter")

  def receive: Receive = {
    case Execute(data) => executeMapping(data.asInstanceOf[Seq[In]], sender())
  }

  def executeMapping(data: Seq[In], requester: ActorRef): Unit = {
    val split = data.grouped(MaxChunkSize).toVector

    context become mappingStage(emptyMappings, split.length, requester)
    for (data <- split) {
      mapReduceWorkRouter ! MappingWork(Chunk(data))
    }
  }

  def mappingStage(accumulated: Mappings[Key, Value], waitingFor: Int, requester: ActorRef): Receive = {

    case MappingResult(mappings) =>
      val left = waitingFor - 1
      val newAccumulated = accumulated append mappings.asInstanceOf[Mappings[Key, Value]]
      context.become(mappingStage(newAccumulated, left, requester))
      if (left == 0)
        executeReducing(newAccumulated, requester)

  }

  def executeReducing(result: Mappings[Key, Value], requester: ActorRef): Unit = {
    val chunks = result.grouped(result.keySet.size / numberOfWorkers).toVector

    context become reducingStage(Map.empty, chunks.length, requester)
    chunks foreach { mappings =>
      mapReduceWorkRouter ! ReducingWork(mappings)
    }
  }

  def reducingStage(accumulated: Map[Key, Reduced], waitingFor: Int, requester: ActorRef): Receive = {
    case ReducingResult(result) =>
      val left = waitingFor - 1
      val newAccumulated = accumulated ++ result.asInstanceOf[Map[Key, Reduced]]
      context.become(reducingStage(newAccumulated, left, requester))
      if (left == 0) {
        requester ! EngineResult(newAccumulated)
        context.stop(self)
      }
  }


}
