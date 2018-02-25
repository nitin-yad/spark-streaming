package impl

import constants.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by nitin.yadav on 24-02-2018.
  */
object SessionManager {

    def getSparkStreamingContext(sec : Int): StreamingContext = {

      val sparkSession = SparkSession.builder.appName(Constants.AppName).master(Constants.Master).getOrCreate()

      /**
        * A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext
        * is stopped (without stopping the SparkContext) before the next StreamingContext is created
        */
      new StreamingContext(sparkSession.sparkContext, Seconds(sec))
    }
}
