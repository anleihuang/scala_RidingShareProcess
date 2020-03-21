package bikeTrip.process

// import spark libraries

import bikeTrip.process.UniqueUserProcess.logInfo
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// import logging
import org.apache.spark.internal.Logging

// import internal packages
import bikeTrip.conf.Conf
import bikeTrip.util.Utils
import bikeTrip.io.{UserReader, TripReader}

object RetentionProcess extends Logging with UserReader with TripReader {


  def _retentionCalc(conf: Conf, spark: SparkSession): Unit = {

    // (current date) Trip DataFrame
    // data path: /input/bike-trips.json/start_date=(current date)/
    val tripDf = readTrip(conf, spark)

    // (current date) Unique User DataFrame
    // data path: outputPath/unique-user/start_date=(current date)/
    val userDf = readUser(conf, spark, conf.dateProcessed())

    // (current date - X days ago) Aggregate Trip (avg sec) DataFrame
    // data path: outputPath/X/start_date=(current date - X days ago)/
    val tripDaysAgoDF = readTripFromDaysAgo(conf, spark)

    // tripDf left join on userDf to get user's first_timestamp
    val joinTripDf = tripDf
      .join(userDf, tripDf.col("user_id") === userDf.col("user_id"), "left")
      .drop(tripDf.col("user_id"))

    // convert rdd to dataset
    import spark.implicits._

    // calculate "user_days_of_join" by converting to unit timestamp (in seconds)
    // and then convert to days by divided 86400
    val userDaysOfJoinDf = joinTripDf
      .withColumn("user_days_of_join",
        floor((unix_timestamp(col("start_timestamp"))
          - unix_timestamp(col("first_timestamp"))) / 86400.0))

    // filter out the userDaysOfJoinDf with only records contain X days ago
    val userFilterDf: DataFrame = conf.daysAgo() match {
      case 1 => userDaysOfJoinDf.filter(col("user_days_of_join") === 1)
      case 3 => userDaysOfJoinDf.filter(col("user_days_of_join") === 3)
      case 7 => userDaysOfJoinDf.filter(col("user_days_of_join") === 7)
      case _ => None
        throw new Exception("invalid input")
    }

    /** get distinct number of users with x days ago
     * | user_id | user_days_of_join |
     * | 445     |    1              |
     */
    val userFilterSelectColsDf: DataFrame = userFilterDf
      .select("user_id", "user_days_of_join")
      .distinct()

    /** aggDf
     * | user_id | subscriber_type | start_station_id | end_station_id | zip_code | avg_duration_sec | user_days_of_join |
     * | 519     | Customer        | 48                | 60            | 95824    | 1958.0           | 1             |
     * | 451     | Customer        | 75                | 50            | null     | 610.0            | null          |
     */
    val aggDf = tripDaysAgoDF.join(userFilterSelectColsDf,
      tripDaysAgoDF.col("user_id") === userFilterSelectColsDf.col("user_id"), "left")
      .drop(userFilterSelectColsDf.col("user_id"))

    val joinFieldsList = AggProcess.fieldsList :+ AggProcess.fieldAvgDurationSec

    /** userRetentionDf
     * | user_id | subscriber_type | start_station_id | end_station_id | zip_code | avg_duration_sec | retention_1 | retention_day3 | retention_day7 |
     * | 519     | Customer        | 48                | 60            | 95824    | 1958.0           | 1           | 0              | 0              |
     * | 519     | Customer        | 60                | 48            | null     | 428.0            | 1           | 0              |  0              |
     * | 451     | Customer        | 75                | 50            | null     | 610.0            | 0           | 0              |  0              |
     */
    val userRetentionDf = aggDf
      .groupBy(joinFieldsList.map(col): _*)
      .agg(
        max(when(col("user_days_of_join") === 1, 1).otherwise(0)).as("retention_day1"),
        max(when(col("user_days_of_join") === 3, 1).otherwise(0)).as("retention_day3"),
        max(when(col("user_days_of_join") === 7, 1).otherwise(0)).as("retention_day7")
      )

    val outputPath = _outputDaysagoWriteToPath(conf)

    logInfo(s"Proccessed unique users .Writing the updated unique user file to $outputPath")

    userRetentionDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("retention-process")
      .getOrCreate()

    val conf = new Conf(args)

    // force to cross join to avoid AnalysisException error
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    _retentionCalc(conf, spark)

  }

}
