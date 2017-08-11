package mapreduce.examples

import scala.util.matching.Regex
import scala.util.{Failure, Try}

object MovieExtractor {

  val movieFormat: Regex = """\{id:(.*), name:(.*)\}""".r

  def apply(movie: String): Try[Movie] = {
    movie match {
      case movieFormat(id, name) => Try(Movie(id.trim.toInt, name.trim))
      case _ => Failure(new MatchError(s"$movie did not match expected format: ${movieFormat.toString()}"))
    }
  }
}
