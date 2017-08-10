package mapreduce.engine.futures

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

object FailAllOnException {

  def apply[T](futures: Seq[Future[T]])(implicit context: ExecutionContext): Future[Seq[T]] = {
    val promise = Promise[Seq[T]]
    futures.foreach {
      _.onComplete {
        case Failure(ex) => promise.failure(ex)
        case other => other
      }
    }
    promise.completeWith(Future.sequence(futures)).future
  }

}
