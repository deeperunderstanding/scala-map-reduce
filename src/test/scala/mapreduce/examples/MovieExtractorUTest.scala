package mapreduce.examples

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Success

class MovieExtractorUTest extends FlatSpec with Matchers {

  "Extracting a Movie from a String" should "result in an Success with an instance of Movie with the respective data" in {

    MovieExtractor("{id: 1, name: 'Strictly Ballroom'}") shouldBe Success(Movie(1, "'Strictly Ballroom'"))
  }

  it should "result in a Failure if the string does not match the movie format" in {

    MovieExtractor("not exactly a movie..") should be a 'failure
  }

  it should " also result in a Failure if the string matches but the id does not contain a number" in {

    MovieExtractor("{id: WRONG!, name: 'The Glitch'}") should be a 'failure
  }

}
