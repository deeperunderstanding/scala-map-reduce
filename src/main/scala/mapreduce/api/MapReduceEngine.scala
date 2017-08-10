package mapreduce.api

import scala.concurrent.Future
import scala.reflect.ClassTag

trait MapReduceEngine {

  def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag](program: MapReduce[I, K, V, R])(data: Seq[I]): Future[EngineResult[K, R]]

}
