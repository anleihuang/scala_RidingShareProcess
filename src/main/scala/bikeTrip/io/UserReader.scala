package bikeTrip.io

// import spark library

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

// import logging library
import org.apache.spark.internal.Logging

// import internal libraries
import bikeTrip.util.Utils
import bikeTrip.conf.Conf

trait UserReader extends Logging {

  /**
   *
   * @param conf
   * @param spark
   * @param currDate
   * @return: return a  unique user DataFrame
   */
  def readUser(conf: Conf, spark: SparkSession, currDate: String): DataFrame = {

    val inputPath = Utils.pathJoin(conf.outputUniqueUserPath(), conf.datePrefix(), currDate)

    logInfo("reading files from %s".format(inputPath))

    val uniqueUserDf: DataFrame = try {
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("first_timestamp", lit(null: StringType))
    }

    uniqueUserDf

  }
}
