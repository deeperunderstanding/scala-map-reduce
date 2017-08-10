package mapreduce.examples

import mapreduce.api.MapReduce
import mapreduce.util.{EngineByName, FileToLines}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global


object MovieRatingAverageApp extends App {

  args match {
    case Array(movies, ratings) => run(movies, ratings)

    case Array(movies, ratings, engine) => run(movies, ratings, engine)

    case arguments =>
      println(s"ERROR: Wrong amount of arguments supplied: ${arguments.length}")
      println("Application takes 2 or 3 parameters:")
      println("[movies] [ratings] ([engine])")
      println("options for engine are: 'multi', 'actor'")

  }

  def run(movieFile: String, ratingsFile: String, engine: String = "single"): Unit = {

    val movies = FileToLines(movieFile) match {
      case Success(movieLines) => movieLines map {
        MovieExtractor(_)
      }
    }

    val ratings = FileToLines(ratingsFile) match {
      case Success(ratingLines) => ratingLines map {
        RatingsExtractor(_)
      }
    }

    val moviesAndRatings = for {
      movie <- movies
      rating <- ratings if movie.id == rating.movieId
    } yield MovieAndRating(movie, rating)

    val startTime = System.currentTimeMillis()
    val result = MapReduce(MovieRatingAverage)(EngineByName(engine))(moviesAndRatings)

    val printResult = result.map { engineResult =>
      println(s"completed in ${System.currentTimeMillis() - startTime} ms")
      engineResult.result.toVector.sortBy(-_._2).foreach { case (key, value) =>
        println(s"$key - $value")
      }
    }

    Await.result(printResult, Duration.Inf)
  }

}
