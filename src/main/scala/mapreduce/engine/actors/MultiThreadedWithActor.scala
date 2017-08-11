package mapreduce.engine.actors

import akka.actor.{ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import mapreduce.api.{EngineResult, MapReduce, MapReduceEngine}
import mapreduce.engine.actors.Messages.Execute

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag


class MultiThreadedWithActor(numberOfWorkers: Int) extends MapReduceEngine {
  override def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag](program: MapReduce[I, K, V, R])(data: Seq[I]): Future[EngineResult[K, R]] = {
    val system = ActorSystem("MapReduceSystem")

    val mapReduce = system.actorOf(Props(new MapReduceExecuter(program)(numberOfWorkers)), name = "master")

    implicit val timeout = Timeout(5.minutes) //TODO pass in Timeout

    val result = (mapReduce ? Execute(data)).mapTo[EngineResult[K, R]] //Initiliaze Worker with data???
    result.onComplete(_ => system.terminate())
    result
  }
}

object MultiThreadedWithActor {

  def apply(numberOfWorkers: Int = 4): MultiThreadedWithActor = new MultiThreadedWithActor(numberOfWorkers)
}


