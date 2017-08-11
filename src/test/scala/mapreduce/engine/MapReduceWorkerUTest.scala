package mapreduce.engine

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import mapreduce.TestProgram
import mapreduce.engine.actors.MapReduceWorker
import mapreduce.engine.actors.Messages.{MappingResult, MappingWork, ReducingResult, ReducingWork}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class MapReduceWorkerUTest extends TestKit(ActorSystem("MapReduceWorker")) with ImplicitSender
  with FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val worker: ActorRef = system.actorOf(Props(
    new MapReduceWorker[String, Char, Int, Int](TestProgram.mapper, TestProgram.reducer)))

  "When a MappingWork message is send to the Worker it" should "respond with a MappingResult message, containing the " +
    "result of the mapping operation on the data in the MappingWork message" in {

    within(500.millis) {
      worker ! MappingWork(TestProgram.testInput)
      expectMsg(MappingResult(TestProgram.expectedMappingResult))
    }

  }

  "When a ReducingWork message is send to the Worker it" should "respond with a ReducingResult message, containing the " +
    "result of the reducing operation for the data in the ReducingWork message" in {

    within(500.millis) {
      worker ! ReducingWork(TestProgram.expectedMappingResult)
      expectMsg(ReducingResult(TestProgram.expectedReducingResult))
    }
  }

}
