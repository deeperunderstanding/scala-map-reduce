package mapreduce.examples

import mapreduce.api.{KeyValue, MapReduce}

object WordCount extends MapReduce[String, String, Int, Int] {

  override def mapper(line: String): Seq[KeyValue[String, Int]] =
    """[\w']+""".r.findAllIn(line).map { word => KeyValue(word, 1) }.toVector


  override def reducer(key: String, values: Seq[Int]): KeyValue[String, Int] =
    KeyValue(key, values.sum)

}
