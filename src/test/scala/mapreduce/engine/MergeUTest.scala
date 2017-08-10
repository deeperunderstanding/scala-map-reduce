package mapreduce.engine

import mapreduce.api.KeyValue
import org.scalatest.{FlatSpec, Matchers}
import Merge._

import scala.collection.immutable.Queue

class MergeUTest extends FlatSpec with Matchers {

  val mappingsA = Map("a" -> Queue(1), "b" -> Queue(1, 1), "c" -> Queue(1))
  val mappingsB = Map("a" -> Queue(1, 1), "b" -> Queue(1), "d" -> Queue(1))
  val mappingsC = Map("a" -> Queue(1, 1, 1), "d" -> Queue(1, 1), "e" -> Queue(1))

  "merge" should "combine a sequence of mappings into a single map with all values for the same key aggregated" in {
    Merge(Seq(mappingsA, mappingsB, mappingsC)) shouldBe Map(
      "a" -> Queue(1, 1, 1, 1, 1, 1),
      "b" -> Queue(1, 1, 1),
      "c" -> Queue(1),
      "d" -> Queue(1, 1, 1),
      "e" -> Queue(1)
    )
  }

  "merging a sequence of key value pairs" should "result in a Map with all values for the same key aggregated" in {
    Merge.keyValuesToMap(
      Seq(
        KeyValue("a", 1),
        KeyValue("a", 1),
        KeyValue("a", 1),
        KeyValue("b", 1),
        KeyValue("b", 1),
        KeyValue("c", 1)
      )
    ) shouldBe Map(
      "a" -> Queue(1, 1, 1),
      "b" -> Queue(1, 1),
      "c" -> Queue(1)
    )
  }

  "appending mappings to an existing map" should "merge the maps, adding new keys and values and adding to the seq of " +
    "values for duplicate keys" in {
    Map("a" -> Queue(1, 1), "b" -> Queue(1)) append Map("b" -> Queue(1), "c" -> Queue(1)) shouldBe
      Map("a" -> Queue(1, 1), "b" -> Queue(1, 1), "c" -> Queue(1))

  }


}
