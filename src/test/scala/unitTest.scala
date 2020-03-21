// import dataframe testing
import com.holdenkarau.spark.testing._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

// import scalatest
import org.scalatest.FunSuite

import java.io.File
import scala.reflect.io.Directory

// import spark library
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// import internal packages
import bikeTrip.conf.Conf
import bikeTrip.io._
import bikeTrip.process._
import bikeTrip.util.Utils.{daysAgoDate, getListFromResourceConf, pathJoin, selectCols}

class ScalaTest extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  test("Utils.pathJoin") {
    assert(pathJoin("gs://test-input", "start_date", "2020-03-01") === "gs://test-input/start_date=2020-03-01/")
  }

  test("Utils.getListFromResourceConf_success") {
    val confFile = "test"
    val confFields = "test"
    assert(getListFromResourceConf(confFile, confFields) == List("language", "version"))
  }

  test("Utils.getListFromResourceConf_file") {
    try {
      val confFile = "./backup.UtilsTest"
      val confFields = "test"
      getListFromResourceConf(confFile, confFields)
    } catch {
      case e: Exception =>
        assert(e.getMessage == "Error happened while parsing test as List[String]")
    }
  }

  test("Utils.selectCols") {

    val conf = new Conf(Seq("--selectcols.conf.file", "test", "--date.processed", "2014-01-01"))
    val cols = "test"

    val inputSchema = List(
      StructField("id", IntegerType, false),
      StructField("language", StringType, false),
      StructField("version", StringType, false),
      StructField("note", StringType, false)
    )
    val inputData = Seq(
      Row(1, "python", "v1.1", "note1"),
      Row(2, "scala", "v2.1", "note2")
    )

    val inputDf = sqlContext.createDataFrame(
      sc.parallelize(inputData),
      StructType(inputSchema)
    )

    val outputSchema = List(
      StructField("language", StringType, false),
      StructField("version", StringType, false)
    )
    val outputData = Seq(
      Row("python", "v1.1"),
      Row("scala", "v2.1")
    )

    val outputDf = sqlContext.createDataFrame(
      sc.parallelize(outputData),
      StructType(outputSchema)
    )

    assertDataFrameEquals(selectCols(conf, cols, inputDf), outputDf)

  }

  test("Utils.daysAgoDate") {

    val conf = new Conf(Seq("--date.processed", "2020-03-08"))

    assert(daysAgoDate(conf, 1) == "2020-03-07")
    assert(daysAgoDate(conf, 3) == "2020-03-05")
    assert(daysAgoDate(conf, 7) == "2020-03-01")
  }

  test("UniqueUserProcess._getUniqueUser") {

    val inputPath = "./src/test/scala/test_files"

    val uniqueUserPath = "./src/test/scala/test_files/unique-user"

    val conf = new Conf(Seq(
                            "--date.processed", "2014-01-02",
                            "--input.trip.path", inputPath,
                            "--output.uniqueUser.path", uniqueUserPath
                            )
                        )

    val temp_outputPath = "./src/test/scala/testout_files"

    import sqlContext.implicits._

    /** create general schema
     * root
     * |-- user_id: string (nullable = true)
     * |-- first_timestamp: string (nullable = true)
     */
    val generalSchema = new StructType()
          .add("user_id", StringType, true)
          .add("first_timestamp", StringType, true)

    val outputData = Seq(
      Row("3", "2014-01-02 12:00:00 UTC"),
      Row("8", "2014-01-01 08:54:00 UTC"),
      Row("5", "2014-01-02 11:30:00 UTC"),
      Row("9", "2014-01-02 17:19:00 UTC"),
      Row("1", "2013-12-30 08:30:00 UTC"),
      Row("4", "2013-12-26 11:21:00 UTC"),
      Row("2", "2014-01-02 11:59:00 UTC")
    )

    val outputDf = sqlContext.createDataFrame(
      sc.parallelize(outputData),
      StructType(generalSchema)
    )

    UniqueUserProcess._getUniqueUser(inputPath, temp_outputPath, conf)

    val result = sqlContext.read.schema(generalSchema).json(temp_outputPath)

    assertDataFrameEquals(result, outputDf)

    // Delete the output dir when finishing the testing
    val outputDir = new Directory(new File(temp_outputPath))
    outputDir.deleteRecursively()

  }


  test("AggProcess._bikeAgg") {

    val inputPath = "./src/test/scala/test_files"

    val conf = new Conf(Seq(
      "--date.processed", "2014-01-02",
      "--input.trip.path", inputPath
    )
    )

    val outputPath = "./src/test/scala/tempout_files"

    /** create general schema
     * root
     * |-- user_id: string (nullable = true)
     * |-- subscriber_type: string (nullable = true)
     * |-- start_station_id: string (nullable = true)
     * |-- end_station_id: string (nullable = true)
     * |-- zip_code: string (nullable = true)
     * |-- avg_duration_sec: double (nullable = true)
     */
    val generalSchema = new StructType()
      .add("user_id", StringType, true)
      .add("subscriber_type", StringType, true)
      .add("start_station_id", StringType, true)
      .add("end_station_id", StringType, true)
      .add("zip_code", StringType, true)
      .add("avg_duration_sec", DoubleType, true)

    val outputData = Seq(
      Row("8", "Subscriber", "14", "6", "95124", 205.0),
      Row("2", "Subscriber", "14", "6", "94133", 205.0),
      Row("9", "Subscriber", "80", "9", "95112", 413.0),
      Row("2", "Subscriber", "35", "38", "94306", 1007.0),
      Row("1", "Subscriber", "14", "4", "94406", 172.0),
      Row("4", "Subscriber", "14", "4", "95012", 172.0),
      Row("3", "Subscriber", "10", "8", "95113", 185.0),
      Row("5", "Subscriber", "12", "11","95112", 255.0)
    )

    val outputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(outputData),
      StructType(generalSchema)
    )

    AggProcess._bikeAgg(conf, spark, outputPath)

    val result = spark.read.schema(generalSchema).json(outputPath)

    assertDataFrameEquals(result, outputDf)

    // Delete the output dir when finishing the testing
    val outputDir = new Directory(new File(outputPath))
    outputDir.deleteRecursively()

  }

  test("Retention._retentionCalc_day1") {

    val inputPath = "./src/test/scala/test_files"

    val uniqueUserPath = "./src/test/scala/test_files/unique-user"

    val outputPath = "./src/test/scala/test_files/output"

    val temp_outputPath = "./src/test/scala/test_files/output/1"

    val conf = new Conf(Seq(
      "--date.processed", "2014-01-02",
      "--input.trip.path", inputPath,
      "--output.uniqueUser.path", uniqueUserPath,
      "--output.path", outputPath
    )
    )

    /** create general schema
     *
     */
    val generalschema = new StructType()
      .add("user_id", StringType, true)
      .add("subscriber_type", StringType, true)
      .add("start_station_id", StringType, true)
      .add("end_station_id", StringType, true)
      .add("zip_code", StringType, true)
      .add("avg_duration_sec", DoubleType, true)
      .add("retention_day1", IntegerType, true)
      .add("retention_day3", IntegerType, true)
      .add("retention_day7", IntegerType, true)

    val outputData = Seq(
      Row("8", "Subscriber", "35", "38", "94406", 86.0, 1, 0 , 0)
    )

    val outputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(outputData),
      StructType(generalschema)
    )

    RetentionProcess._retentionCalc(conf, spark)

    val result = spark.read.schema(generalschema).json(temp_outputPath + "/start_date=*/*.json")

    assertDataFrameEquals(result, outputDf)

    // Delete the output dir when finishing the testing
    val outputDir = new Directory(new File(temp_outputPath))
    outputDir.deleteRecursively()

  }

  test("Retention._retentionCalc_day3") {

    val inputPath = "./src/test/scala/test_files"

    val uniqueUserPath = "./src/test/scala/test_files/unique-user"

    val outputPath = "./src/test/scala/test_files/output"

    val temp_outputPath = "./src/test/scala/test_files/output/3"

    val conf = new Conf(Seq(
      "--date.processed", "2014-01-02",
      "--input.trip.path", inputPath,
      "--output.uniqueUser.path", uniqueUserPath,
      "--output.path", outputPath,
      "--days.ago", "3"
    )
    )

    /** create general schema
     *
     */
    val generalschema = new StructType()
      .add("user_id", StringType, true)
      .add("subscriber_type", StringType, true)
      .add("start_station_id", StringType, true)
      .add("end_station_id", StringType, true)
      .add("zip_code", StringType, true)
      .add("avg_duration_sec", DoubleType, true)
      .add("retention_day1", IntegerType, true)
      .add("retention_day3", IntegerType, true)
      .add("retention_day7", IntegerType, true)

    val outputData = Seq(
      Row("1", "Subscriber", "80", "9", "95124", 984.0, 0, 1 , 0)
    )

    val outputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(outputData),
      StructType(generalschema)
    )

    RetentionProcess._retentionCalc(conf, spark)

    val result = spark.read.schema(generalschema).json(temp_outputPath + "/start_date=*/*.json")

    assertDataFrameEquals(result, outputDf)

    // Delete the output dir when finishing the testing
    val outputDir = new Directory(new File(temp_outputPath))
    outputDir.deleteRecursively()

  }

  test("Retention._retentionCalc_day7") {

    val inputPath = "./src/test/scala/test_files"

    val uniqueUserPath = "./src/test/scala/test_files/unique-user"

    val outputPath = "./src/test/scala/test_files/output"

    val temp_outputPath = "./src/test/scala/test_files/output/7"

    val conf = new Conf(Seq(
      "--date.processed", "2014-01-02",
      "--input.trip.path", inputPath,
      "--output.uniqueUser.path", uniqueUserPath,
      "--output.path", outputPath,
      "--days.ago", "7"
    )
    )

    /** create general schema
     *
     */
    val generalschema = new StructType()
      .add("user_id", StringType, true)
      .add("subscriber_type", StringType, true)
      .add("start_station_id", StringType, true)
      .add("end_station_id", StringType, true)
      .add("zip_code", StringType, true)
      .add("avg_duration_sec", DoubleType, true)
      .add("retention_day1", IntegerType, true)
      .add("retention_day3", IntegerType, true)
      .add("retention_day7", IntegerType, true)

    val outputData = Seq(
      Row("4", "Subscriber", "12", "11", "95012", 1200.0, 0, 0 , 1)
    )

    val outputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(outputData),
      StructType(generalschema)
    )

    RetentionProcess._retentionCalc(conf, spark)

    val result = spark.read.schema(generalschema).json(temp_outputPath + "/start_date=*/*.json")

    assertDataFrameEquals(result, outputDf)

    // Delete the output dir when finishing the testing
    val outputDir = new Directory(new File(temp_outputPath))
    outputDir.deleteRecursively()
  }

}
