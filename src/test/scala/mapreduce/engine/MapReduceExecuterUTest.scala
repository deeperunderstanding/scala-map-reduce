package mapreduce.engine

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import mapreduce.MockProgram
import mapreduce.api.EngineResult
import mapreduce.engine.actors.MapReduceExecuter
import mapreduce.engine.actors.Messages.Execute
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class MapReduceExecuterUTest extends TestKit(ActorSystem("MapReduceExecuter")) with ImplicitSender
  with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val executer: ActorRef = system.actorOf(Props(
    new MapReduceExecuter[String, Char, Int, Int](MockProgram)(1)))

  "Sending the MapReduceExecuter an Execute message with data" should "cause it to perform a map-reduce transformation on the " +
    "input data and eventually respond with an EngineResult containing the correct result" in {

    within(1.second) {
      executer ! Execute(MockProgram.testInput)
      expectMsg(EngineResult(MockProgram.expectedReducingResult))
    }

  }

}
