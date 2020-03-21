package bikeTrip.process

// import spark library

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

// import logging
import org.apache.spark.internal.Logging

// import internal packages
import bikeTrip.io.{TripReader, UserReader}
import bikeTrip.conf.Conf
import bikeTrip.util.Utils


object UniqueUserProcess extends Logging with TripReader with UserReader {

  val spark = SparkSession
    .builder()
    .appName("Unique-user-process")
    .getOrCreate()

  /**
   * get the new user list from the new trip, union the new user list
   * to the existing one and get the distinct user_id
   *
   * @param inputTripPath
   * @param outputUniqueUserPath
   * @param conf
   */
  def _getUniqueUser(inputTripPath: String, outputUniqueUserPath: String, conf: Conf): Unit = {

    val tripDf = readTrip(conf, spark)

    val currUniqueUserDf = readUser(conf, spark, Utils.daysAgoDate(conf, 1))

    val newUsersDf = Utils.selectCols(conf, "uniqueUser", tripDf)
      .withColumnRenamed("start_timestamp", "first_timestamp")

    val newUniqueUserDf = currUniqueUserDf
      .unionByName(newUsersDf)
      .groupBy("user_id")
      .agg(min("first_timestamp").as("first_timestamp"))

    logInfo(s"Proccessed unique users .Writing the updated unique user file to $outputUniqueUserPath")

    newUniqueUserDf.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(outputUniqueUserPath)

  }

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)

    val inputTripPath = Utils.pathJoin(conf.inputTripPath(), conf.datePrefix(), conf.dateProcessed())

    val outputUniqueUserPath = Utils.pathJoin(conf.outputUniqueUserPath(), conf.datePrefix(), conf.dateProcessed())

    _getUniqueUser(inputTripPath, outputUniqueUserPath, conf)

  }

}
