package impl

import java.io.{InputStreamReader, BufferedReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import constants.Constants._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


/**
  * Created by nitin.yadav on 25-02-2018.
  */
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){

  override def onStart(): Unit = {

    /**
      * Start thread that receives data
      */
    new Thread("Socket receiver") {
      override def run(): Unit ={
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = ???

  private def receive(): Unit = {

    var socket: Socket = null
    var input: String = null
    try{
      socket = new Socket(host, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      input = reader.readLine()
      while(!isStopped() && input != null){
        store(input)
        input = reader.readLine()
      }
      reader.close()
      socket.close()

      /**
        * restart in an attempt to connect again
        */
      restart("Trying to connect again")
    } catch{
      case e: ConnectException =>
        restart(s"Error connecting to socket: $host:$port", e)

      case t: Throwable =>
        restart("Error in receiving data", t)
    }
  }
}

object CustomReceiver {

  def main(args: Array[String]): Unit = {

    val streamingContext = SessionManager.getSparkStreamingContext(BatchInterval)
    val lines = streamingContext.receiverStream(new CustomReceiver(HostName, Port))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_+_)
    wordCounts.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
