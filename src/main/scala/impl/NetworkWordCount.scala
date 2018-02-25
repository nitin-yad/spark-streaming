package impl

import constants.Constants._

/**
  * Created by nitin.yadav on 24-02-2018.
  */
object NetworkWordCount {


  def main(args: Array[String]) : Unit = {

    val streamingContext = SessionManager.getSparkStreamingContext(BatchInterval)

    val linesDStream = streamingContext.socketTextStream(HostName, Port)
    val wordsDStream = linesDStream.flatMap(_.split(" "))

    val pairsDStream = wordsDStream.map(word => (word, 1))
    val wordCountsDStream = pairsDStream.reduceByKey(_+_)

    /**
      * Print first 10 elements of each RDD generated in this DStream to the console
      */
    wordCountsDStream.print()

    /**
      * When previous line are executed, Spark Streaming only sets up the computation, no rwal processin has
      * started yet.
      */
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
