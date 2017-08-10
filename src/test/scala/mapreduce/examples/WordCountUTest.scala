package mapreduce.examples


import mapreduce.api.KeyValue
import org.scalatest.{FlatSpec, Matchers}

class WordCountUTest extends FlatSpec with Matchers {

  "mapping a line of text" should "map every word in the row to the number 1" in {
    WordCount.mapper("Hello, this is a Test") should contain theSameElementsAs Seq(
      KeyValue("Hello", 1),
      KeyValue("this", 1),
      KeyValue("is", 1),
      KeyValue("a", 1),
      KeyValue("Test", 1)
    )
  }

  "reducing values for a word" should "sum all the values for that word" in {
    WordCount.reducer("test", Seq(1, 1, 1, 1, 1)) shouldBe KeyValue("test", 5)
  }


}
