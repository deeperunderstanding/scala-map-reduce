package mapreduce.examples

import mapreduce.api.KeyValue
import org.scalatest.{FlatSpec, Matchers}

class MovieRatingAverageUTest extends FlatSpec with Matchers {


  "mapping a movie and rating" should "result in a key value pair representing the movie name and the rating" in {
    val movieAndRating = MovieAndRating(
      Movie(1, "The Matrix"),
      Rating(12, 1, 9.5f)
    )

    MovieRatingAverage.mapper(movieAndRating) shouldBe Seq(KeyValue(movieAndRating.movie.name, movieAndRating.rating.rating))
  }

  "reducing a movie name and sequence of ratings" should "result in the movie name and the average of all ratings" in {
    val result = MovieRatingAverage.reducer("The Movie", Seq(9, 8, 7.5f, 9, 8.5f))

    result shouldBe KeyValue("The Movie", 8.4f)
  }


}
