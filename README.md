# Scala Map-Reduce

### Original Assignment

> - Write an engine to execute MapReduce programs. It can be written in either Python, Java or Scala. It doesn't necessarily have to be any similar to the examples provided, the important is the API and the output.

> - Write a version of the word-count MapReduce program exemplified above, runnable by your engine.

> - Write a new MapReduce program that given two datasets (movies and ratings), returns the AVG rating of each movie (in the format: Movie Name, Rating).

> - Write a second engine (with same API and requirements) that makes use of more than one core. You should be able to run the same program in either engine without changes.

## What is a Map-Reduce Engine?

Map-Reduce in general is a programming model for parallel computing of large amounts of data, applicable to certain types of problems.

A map-reduce program is generally defined by two functions, a __mapping function__ and a __reduce function__, where the mapping function takes a _line of input data_, and assigns it a __key-value pair__, whereas the reduce function takes a key and a list of all values for that key, generated by the mapping function, and 'reduces' them to a single value, and returns the key together with that new value.

The Map-Reduce Engine itself is a program that is responsible for executing the map-reduce-program (the mapping and reducing function) on the input data and output the result, it can generally be broken down into three main phases:

1. __Mapping__
  - the input data is distributed to mapping processes which execute the mapping function, provided by the user of the engine, on the data.
  - ideally the mapping processes are executed in parallel on chunks of the data.
  - the mapping process can aggregate the result of the mapping function and sort them by key, associating every unique key with the list of values resulting from the mapping function which share the same key

2. __Shuffle__
  - after mappings are complete the result of the individual processes need be sorted by key and all values for a key have to be collected.
  - this step needs to complete before the reducing phase can start since the reducing phase is executed once for every key and its set of values.
  - the shuffle phase is irrelevant to the user, but it's performance has significant impact on the performance of the entire system
3. __Reduce__
  - the pairs of keys and lists of values are distributed to the reduce process which invokes the reduce function, provided by the user, for every key and the associated list of values.
  - this can also be done completely in parallel, the results just need to be collected and returned to the user.

On an example: Consider a map-reduce program, that counts the appearance of each letter in a sentence or a word. The word would be the input to the mapping function, which would go through the sentence letter by letter and return a key-value pair with the letter as the key and the number 1 as the value.

```
override def mapper(input: String): Seq[KeyValue[Char, Int]] =
  input.map(char => KeyValue(char, 1))
```

On the word 'happy' the function would produce the pairs `(h,1)`, `(a,1)`, `(p,1)`, `(p,1)`, `(y,1)`. Afterwards the engine would sort those pairs by key and aggregate the values for the same key, resulting for this example in
```
  h -> 1
  a -> 1
  p -> 1, 1
  y -> 1
```
This result could then be handed to the reduce function which would sum the values for each key
```
override def reducer(key: Char, values: Seq[Int]): KeyValue[Char, Int] =
    KeyValue(key, values.sum)
```
which would ultimately result in
```
 h -> 1
 a -> 1
 p -> 2
 y -> 1
```


---

## Project Overview


### Structure
`SBT` based project with following packages

  - __src/main/scala/mapreduce__ - root for application code
    - __api__ - interfaces and classes for describing map reduce programs and the interface to the engine
    - __engine__ - general components used in map-reduce and implementations of the engine interface
      - __single__ - simple single threaded implementation of the engine
      - __future__ - a parallel implementation based on Futures
      - __actor__ - a parallel implementation based on Actors
    - __examples__ - example implementations of a map-reduce program with executable Apps
    - __util__ - a few objects for utility tasks
  - __src/test/scala/mapreduce__
    - __engine__ - unit tests for engine components and the implementations
    - __examples__ - unit tests for example programs

Dependencies are: from `build.sbt`
```Scala
scalaVersion := "2.12.3"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

lazy val akkaVersion = "2.5.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
```

### API

#### Map Reduce Interface
Implement to describe map-reduce programs for the engine. The trait accepts four type parameters for the Input data, the Key, the Values emitted from the mapper and the Values emitted from the reducer.

```Scala
trait MapReduce[In, Key, Value, Reduced] {

  def mapper(input: In): Seq[KeyValue[Key, Value]]

  def reducer(key: Key, values: Seq[Value]): KeyValue[Key, Reduced]

}
```
The `KeyValue` class is just a more expressive Tuple of a Key and Value type

```Scala
case class KeyValue[K, V](key: K, value: V)

```

Example Word Count Program
```Scala
object WordCount extends MapReduce[String, String, Int, Int] {

  override def mapper(line: String): Seq[KeyValue[String, Int]] =
    """[\w']+""".r.findAllIn(line).map { word => KeyValue(word, 1) }.toVector


  override def reducer(key: String, values: Seq[Int]): KeyValue[String, Int] =
    KeyValue(key, values.sum)

}
```

#### Engine Interface
The Interface provides a type parameter with an implicit `ClassTag` corresponding to each Type of the map-reduce program, in case type erasure becomes a problem with a specific engine implementation. Result of the engine is a `Future` so the receiver can decide to wait for the result or continue in a non blocking fashion

```Scala
trait MapReduceEngine {

  def apply[I: ClassTag, K: ClassTag, V: ClassTag, R: ClassTag]
  (program: MapReduce[I, K, V, R])(data: Seq[I]): Future[EngineResult[K, R]]

}
```

### Engine

#### Single Threaded Version
The single threaded version of a map reduce 'engine' can be seen simply as a function accepting the input data, sequentially applying the mapping function, collecting the key-value pairs and ordering them by key, and then sequentially applying the reducing function on the pairs of keys and accumulated values. The result of that operation is a Map representing all keys and their values after reduction.

For small datasets the single threaded version is the most efficient one, since it bears no overhead of instantiating any sort of _Thread_ or parallelized work.
since no side effects occur and this version of the engine requires no state and is simply a combination of functions on the input data the 'engine' is itself simply a function

Regarding the interface - the Future return type also works for the single threaded version that blocks the main thread while executing the map-reduce, by using `Future.fromTry` to wrap an operation into a try that will be executed immediately in the current thread.

#### Multi Threaded Version with Futures

It appears the nature of the data the map-reduce algorithm operates on is very conveniently to parallelize for the mapping and reducing phase, which led me to investigate the first attempt to a map-reduce engine utilizing more than one processor core with Scala's `Future`, since it offers a simple approach to parallel executions.

Since `Future` mostly appears to be used for one-off parallel executions with limited means for control and communication and apparently meant to encapsulate a future result that can be reacted on with either callback-functions, mapping and transformation operations or by polling, it seemed a logical approach to split the input data into equal parts and distribute each to a Future to apply the mapping and a partial collection of the result in a number of separate Threads.

it appeared that the shuffle phase is a bottleneck in this version as the results of each `Future` needs to be collected and merged before starting the reducing phase, which leads to having to wait for all Futures before joining the data in a single thread. Fortunately Futures offer a convenient way with `Future.sequence` to gather results of a collection of Futures which return results of the same type by combining them to a single Future.

For the reducing phase the aggregated keys and values from the mapping phase are split into an equal amount of chunks over the key-set and are each handed over to a `Future` for execution in parallel. Getting new threads from the thread-pool again and assigning the work for the reducing phase, bears some overhead though.

A definite plus is that we can continue working and transforming the potential result without ever having to block. The application receiving the result from the engine can decide how to handle the result and if and when to block and react. After some consideration though it appears this approach is suboptimal

#### Multi Threaded Version with Actors

The lack of adaptability and general performance, for this type of problem, of the `Future` based approach led to investigate a third version of the map-reduce engine, run on _Akka's_ `Actor` system, for describing a distributed system in a message driven manner. The main advantage is that Worker and supervising Thread can easily communicate via messages without having to worry about accessing and locking common resources or nearly any concurrency concerns at all. This is a big advantage which can be used to distribute the work more flexible and receive results asynchronously

A simple implementation using a number of _Worker_ threads to execute mapping, collecting and reducing, has similarity to the future based version, except that the mapping and reducing can be done on a number of worker-threads that keep running and executes work based on what message they receive. Also this approach allows for better tuning in regards of chunk size and number of workers since those factors can be easily separated now. Some tweaking of the number of workers and the chunk size can lead to noticeable increase in performance over the single threaded and `Future` based version.

In this version the shuffle phase is still implemented as a merge of results in a single thread (not the main thread), unlike the `Future` based approach though, which has to wait for all work to be finished before merging, the results can now be merged sequentially but asynchronous from the work process and more work can be done while merging is underway. This is done by the supervising Actor of the `MapReduceWorker`, the `MapReduceExecuter` which distributes the map- and reduce work to the workers and receives the results as messages for aggregation.

> __Note__ :  Every `Actor` defines it's behavior by overriding a `receive` function, which returns a `PartialFunction` that describes what messages are reacted on and what happens for every type of message. The `Receive` type in `Actor` is defined as a `PartialFunction[Any, Unit]` which means that virtually 'everything' in Scala can be used as a message. Furthermore the `Actor` can change the currently used `PartialFunction` describing it's behaviour with the command `context.become`, which not only allows to treat an `Actor` as a flexible state-machine, but also to collect data in an `Actor` without having to use a `var` or a mutable collection. See `MapReduceExecuter` as an example.


Once all work is done the result is send to the original requester. Since message can only be send to an `ActorRef`, to access the result from an Actor system the so called 'ask pattern' is used, which sends a message to the `MapReduceExecuter` which causes it to execute the mapping, merging and reducing on the given map-reduce program on the provided data, and to respond with a message containing the result, without the sender having to be an `Actor`. The 'ask pattern' cause the result to be of a type of `Future` which can simply be returned from the engine and responsibility for handling the result is given back to process executing the engine.

#### Comparison
Runtime comparison of the engine implementations on the WordCount program over two datasets of different sizes (collected works of Shakespeare (~5MB) and collected works of Shakespeare * 5 (~25MB)) on the same environment

```
runtimes for engine: mapreduce.engine.single.SingleThreaded$
on data set of size: 175376 lines
on average: 593 ms
-------------------------------------------------
runtimes for engine: mapreduce.engine.single.SingleThreaded$
on data set of size: 876880 lines
on average: 3061 ms
-------------------------------------------------
runtimes for engine: mapreduce.engine.futures.MultiThreadedWithFutures
on data set of size: 175376 lines
on average: 325 ms
-------------------------------------------------
runtimes for engine: mapreduce.engine.futures.MultiThreadedWithFutures
on data set of size: 876880 lines
on average: 2113 ms
-------------------------------------------------
runtimes for engine: mapreduce.engine.actors.MultiThreadedWithActor
on data set of size: 175376 lines
on average: 336 ms
-------------------------------------------------
runtimes for engine: mapreduce.engine.actors.MultiThreadedWithActor
on data set of size: 876880 lines
on average: 1240 ms
-------------------------------------------------
```

---

### Using the example Apps
The provided example Apps `WordCountApp` and `MovieRatingAverageApp` can be run from the `sbt` shell via
```
sbt > runMain mapreduce.examples.WordCountApp file.txt actor
```
The `WordCountApp` takes 1 or 2 parameters, the first being the file to read in which the wordc-ount is being performed on, the second argument is optional chooses the engine implementation being used for the App. The options for that are `single`, `multi` and `actor`, with `single` being the default argument if none is supplied. They respectively run the map-reduce on the `SingleThreaded` version, the `Future` based version or the `Actor` based version.

To run the `MovieRatingAverageApp` respectively use following command in `sbt` shell, with the the first argument being the file containing all movies and the second argument being the file containing the ratings.
```
sbt > runMain mapreduce.examples.MovieRatingAverageApp movies.txt ratings.txt actor
```



### TODO

- Error handling in the Actor based engine
- Some missing tests
- More detailed performance analysis

### Observations and Thoughts

- Multi threaded versions take longer than the single threaded one on small data sets, it seems like the overhead of creating threads (or getting them from the thread-pool) and copying the data is not worth considered that the actual computation time of the mapping and reducing operations is not very extensive.

- Scala collections, the immutable as well as mutable ones have different performance characteristics. `Queue` has the best performance for appending values of all the immutable collections and is chosen to accumulate the sequence of values resulting from the mapping functions for a specific key. If a mutable collection for collecting values would have been chosen, ListBuffer would be most efficient. Decided to stick with immutable collections for safety in parallel processing, careful use of mutable collections might increase performance

- After some experimentation with different ways of merging the data and comparing runtimes, it appears the implementation of the shuffle-phase seems to have the most significant impact on performance along with optimization of the number of workers and the chunk size, the way the data is collected and merged from the mapping processes needs to be considered especially when implementing a map-reduce environment.

- The Actor based version could be extended to implement a distributed shuffling of the key-set between the worker threads, which might impact the performance positively.

- Considering the overhead a splitting strategy needs to be chosen in respect to the size of the data, a minimum chunk size should be determined to avoid starting many threads for small amounts of data

- At some point in increasing data size a `Stream` based approach (lazy evaluating lists) could be considered, to avoid having to load all input data into memory before being able to execute the map-reduce. Regardless of that the mapping needs to complete over the entire input data before reducing can commence, the mapping results would need to be held in memory or moved to persistent storage again.
