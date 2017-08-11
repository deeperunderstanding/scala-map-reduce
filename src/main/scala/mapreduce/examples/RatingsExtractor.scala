package mapreduce.examples

import scala.util.matching.Regex
import scala.util.{Failure, Try}

object RatingsExtractor {

  val ratingFormat: Regex = """\{user_id:(.*), movie_id:(.*), rating:(.*)\}""".r

  def apply(rating: String): Try[Rating] = {
    rating match {
      case ratingFormat(userid, movieid, ratingValue) => Try(Rating(userid.trim.toInt, movieid.trim.toInt, ratingValue.trim.toFloat))
      case _ => Failure(new MatchError(s"$rating did not match expected format: ${ratingFormat.toString()}"))
    }
  }
}
