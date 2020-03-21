package bikeTrip.process

// import spark libraries

import bikeTrip.process.UniqueUserProcess.logInfo
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// import logging library
import org.apache.spark.internal.Logging

// import internal packages
import bikeTrip.conf.Conf
import bikeTrip.util.Utils
import bikeTrip.io.TripReader

object AggProcess extends Logging with TripReader {

  // the variables is also used by the RetentionProcess object
  val fieldsList = List("user_id", "subscriber_type", "start_station_id", "end_station_id", "zip_code")

  val fieldAvgDurationSec = "avg_duration_sec"

  /**
   *
   * @param conf
   * @param spark
   * @param outputPath
   */

  def _bikeAgg(conf: Conf, spark: SparkSession, outputPath: String): Unit = {

    val tripDf = readTrip(conf, spark)

    val tripAggDf = tripDf
      .groupBy(fieldsList.map(col): _*)
      .agg(
        avg(col("duration_sec")).as(fieldAvgDurationSec)
      )

    logInfo(s"Proccessed unique users .Writing the updated unique user file to $outputPath")

    tripAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("bike-aggregate-process")
      .getOrCreate()

    val conf = new Conf(args)

    val outputPath = Utils.pathJoin(conf.outputPath() + "/agg", conf.datePrefix(), conf.dateProcessed())

    _bikeAgg(conf, spark, outputPath)

  }

}
