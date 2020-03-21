package bikeTrip.io

// import spark library
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{lit, col}
import org.apache.spark.sql.types.{StringType, DoubleType}

// import logging library
import org.apache.spark.internal.Logging

// import Java date and time library
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// import internal libraries
import bikeTrip.conf.Conf
import bikeTrip.util.Utils


trait TripReader extends Logging {

  /**
   *
   * @param conf
   * @param spark
   * @return: a trip DataFrame with selected columns
   */

  def readTrip(conf: Conf, spark: SparkSession): DataFrame = {

    val inputPath = Utils.pathJoin(conf.inputTripPath(), conf.datePrefix(), conf.dateProcessed())

    logInfo(s"Process Trip data. Reading files from $inputPath")

    // do a data quality check & null fill in case some data is missing
    val tripFullDf: DataFrame = try {
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("duration_sec", lit(null: DoubleType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("start_timestamp", lit(null: StringType))
    }

    val tripSelectedDf = Utils.selectCols(conf, "trip", tripFullDf)

    tripSelectedDf

  }

  /**
   *
   * @param conf
   * @param spark
   * @return: a trip DataFrame with processed day X days ago
   */
  def readTripFromDaysAgo(conf: Conf, spark: SparkSession): DataFrame = {

    val inputPath = _outputDaysagoReadFromPath(conf)

    logInfo(s"reading files from $inputPath")

    val tripDaysagoDf: DataFrame = try {
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("avg_duration_sec", lit(null: DoubleType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
    }

    tripDaysagoDf

  }

  /**
   *
   * @param conf
   * @return: a string of the read in path
   */

  def _outputDaysagoReadFromPath(conf: Conf): String = {

    val daysagoDate = Utils.daysAgoDate(conf, conf.daysAgo().toInt)

    logInfo(s"reading file from date = $daysagoDate")

    val inputPath: String = conf.daysAgo() match {
      case 1 => Utils.pathJoin(conf.outputPath() + "/agg", conf.datePrefix(), daysagoDate)
      case 3 => Utils.pathJoin(conf.outputPath() + "/agg", conf.datePrefix(), daysagoDate)
      case 7 => Utils.pathJoin(conf.outputPath() + "/agg", conf.datePrefix(), daysagoDate)
      case _ => None
        throw new Exception("invalid input")
    }

    inputPath

  }

  /**
   *
   * @param conf
   * @return: a string of the outputPath
   */

  def _outputDaysagoWriteToPath(conf: Conf): String = {

    val daysagoDate = Utils.daysAgoDate(conf, (conf.daysAgo()).toInt)

    logInfo("writing file to [%s] days ago folder, which the date is %s".format(conf.daysAgo(), daysagoDate))

    val outputPath: String = conf.daysAgo() match {
      case 1 => Utils.pathJoin(conf.outputPath() + "/1", conf.datePrefix(), daysagoDate)
      case 3 => Utils.pathJoin(conf.outputPath() + "/3", conf.datePrefix(), daysagoDate)
      case 7 => Utils.pathJoin(conf.outputPath() + "/7", conf.datePrefix(), daysagoDate)
      case _ => None
        throw new Exception("invalid input")
    }

    outputPath

  }

}
