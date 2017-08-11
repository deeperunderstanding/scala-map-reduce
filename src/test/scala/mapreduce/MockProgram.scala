package mapreduce

import mapreduce.api.{KeyValue, MapReduce}

import scala.collection.immutable.Queue

object MockProgram extends MapReduce[String, Char, Int, Int] {

  val testInput = Seq(
    "This",
    "is",
    "a",
    "Test"
  )

  val expectedMappingResult = Map(
    'e' -> Queue(1), 's' -> Queue(1, 1, 1),
    'T' -> Queue(1, 1), 't' -> Queue(1),
    'a' -> Queue(1), 'i' -> Queue(1, 1), 'h' -> Queue(1))

  val expectedReducingResult = Map('e' -> 1, 's' -> 3, 'T' -> 2, 't' -> 1, 'a' -> 1, 'i' -> 2, 'h' -> 1)


  override def mapper(input: String): Seq[KeyValue[Char, Int]] =
    input.map(char => KeyValue(char, 1))

  override def reducer(key: Char, values: Seq[Int]): KeyValue[Char, Int] =
    KeyValue(key, values.sum)
}
