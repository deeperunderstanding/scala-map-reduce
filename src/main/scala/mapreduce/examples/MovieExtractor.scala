package mapreduce.examples

import scala.util.matching.Regex

object MovieExtractor {

  val movieFormat: Regex = """\{id:(.*), name:(.*)\}""".r

  def apply(movie: String): Movie = {
    movie match {
      case movieFormat(id, name) => Movie(id.trim.toInt, name.trim)
    }
  }
}
