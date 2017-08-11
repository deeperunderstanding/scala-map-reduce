package mapreduce.examples

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class RatingsExtractorUTest extends FlatSpec with Matchers {

  "extracting a Rating from a String" should "result in a Success of a Rating class if the string " +
    "matches the rating pattern" in {

    RatingsExtractor("{user_id: 1, movie_id: 1, rating: 2.5}") shouldBe Success(Rating(1, 1, 2.5f))
  }

  it should "result in a Failure if the String doesn't match the Rating pattern" in {

    RatingsExtractor("tis be no rating") should be a 'failure
  }

  it should "result in a Failure if the String matches the pattern but any of the values are not numbers" in {

    RatingsExtractor("{user_id: WRONG!, movie_id: 1, rating: 2.5}") should be a 'failure
    RatingsExtractor("{user_id: 1, movie_id: WRONG!, rating: 2.5}") should be a 'failure
    RatingsExtractor("{user_id: WRONG!, movie_id: 1, rating: WRONG!}") should be a 'failure
  }

}
