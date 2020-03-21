package bikeTrip.util

// import typesafe configfactory library

import com.typesafe.config.ConfigFactory

// import spark library
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

// import logging library
import org.apache.spark.internal.Logging

// import Java conversion
import scala.collection.JavaConversions._

// import Java date and time library
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// import internal libraries
import bikeTrip.conf.Conf

object Utils extends Logging {

  /**
   * to get a new path with dateprefix and dateprocessed appended
   *
   * @param inputPath     : a string of the input path
   * @param datePrefix    : a string of the prefix date
   * @param dataProcessed : a string of the processed date
   * @return: a string of the new path
   */
  def pathJoin(inputPath: String, datePrefix: String, dataProcessed: String): String = {
    s"$inputPath/$datePrefix=$dataProcessed/"
  }

  /**
   * to parse the list from the default.conf file and return a list of column fields
   *
   * @param confFile   : trip or uniqueUser or station
   * @param confFields :
   * @return: a list of column fields
   */
  def getListFromResourceConf(confFile: String, confFields: String): List[String] = {
    try {

      logInfo(s"reading conf file from %s".format(confFile))

      val conf = ConfigFactory.load(confFile)
      conf.getStringList(confFields).toList
    } catch {
      case e: Exception =>
        logError(s"Error happened while parsing $confFields as List[String]")
        List[String]()
    }
  }

  /**
   * to get a DataFrame which contains only the selected columns
   *
   * @param conf    :
   * @param cols    :
   * @param inputDf : spark readin DataFrame
   * @return: a DataFrame with selectec columns
   */
  def selectCols(conf: Conf, cols: String, inputDf: DataFrame): DataFrame = {
    val fields = getListFromResourceConf(conf.selectColsFromResourceConf(), cols).map(col)
    val outputDf = inputDf.select(fields: _*)

    outputDf
  }

  /**
   *
   * @param conf
   * @param daysAgo
   * @return: a string of the date which is the processed date minus X days ago
   */
  def daysAgoDate(conf: Conf, daysAgo: Int): String = {
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
    val dataProcessed = DateTime.parse(conf.dateProcessed(), dateFormat)
    dateFormat.print(dataProcessed.minusDays(daysAgo))
  }

}
