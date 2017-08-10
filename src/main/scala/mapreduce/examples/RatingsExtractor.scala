package mapreduce.examples

import scala.util.matching.Regex

object RatingsExtractor {

  val ratingFormat: Regex = """\{user_id:(.*), movie_id:(.*), rating:(.*)\}""".r

  def apply(movie: String): Rating = {
    movie match {
      case ratingFormat(userid, movieid, rating) => Rating(userid.trim.toInt, movieid.trim.toInt, rating.trim.toFloat)
    }
  }
}
