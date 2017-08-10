package mapreduce.examples

import mapreduce.api.{KeyValue, MapReduce}

object MovieRatingAverage extends MapReduce[MovieAndRating, String, Float, Float] {
  override def mapper(input: MovieAndRating): Seq[KeyValue[String, Float]] =
    Seq(KeyValue(input.movie.name, input.rating.rating))


  override def reducer(key: String, values: Seq[Float]): KeyValue[String, Float] =
    KeyValue(key, values.sum / values.length)
}

case class Movie(id: Int, name: String)

case class Rating(userId: Int, movieId: Int, rating: Float)

case class MovieAndRating(movie: Movie, rating: Rating)




