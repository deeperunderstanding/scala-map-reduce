package mapreduce.api


import scala.concurrent.Future
import scala.reflect.ClassTag

trait MapReduce[In, Key, Value, Reduced] {

  def mapper(input: In): Seq[KeyValue[Key, Value]]

  def reducer(key: Key, values: Seq[Value]): KeyValue[Key, Reduced]

}

object MapReduce {
  def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag]
  (program: MapReduce[I, K, V, R])(engine: MapReduceEngine)(data: Seq[I]): Future[EngineResult[K, R]] = engine(program)(data)

}