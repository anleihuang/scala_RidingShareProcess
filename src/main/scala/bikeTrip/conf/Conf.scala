package bikeTrip.conf

// import scala command-line arguments parsing library

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 *
 * @param args
 * --days.ago
 * --date.prefix
 * --date.processed
 * --input.trip.path
 * --input.metadata.path
 * --output.uniqueUser.path
 * --output.path
 * --stage
 * --selectcols.conf.file
 */

class Conf(args: Seq[String]) extends ScallopConf(args) with Serializable {

  val daysAgo: ScallopOption[Int] = opt[Int](
    name = "days.ago",
    descr = "X number of days ago data to be processed",
    required = false,
    default = Option(1)
  )

  val datePrefix: ScallopOption[String] = opt[String](
    name = "date.prefix",
    descr = "get the date prefix to join to the new path",
    required = false,
    default = Option("start_date")
  )

  val dateProcessed: ScallopOption[String] = opt[String](
    name = "date.processed",
    descr = "to get the processed date with yyyy-mm-dd format",
    required = true
  )

  val inputTripPath: ScallopOption[String] = opt[String](
    name = "input.trip.path",
    descr = "input of the bike trip data",
    required = false,
    default = stage() match {
      case "dev" => Option("gs://bike-trip/input/bike-trips.json")
      case "qa" => Option("gs://bike-trip/input/bike-trips.json")
      case "prod" => Option("gs://bike-trip/input/bike-trips.json")
      case _ => None
        throw new Exception("Stage not found. Please define the stage: dev, qa or prod")
    }
  )

  val inputMetadataPath: ScallopOption[String] = opt[String](
    name = "input.metadata.path",
    descr = "path of the input metadata",
    required = false,
    default = stage() match {
      case "dev" => Option("gs://bike-trip/meta")
      case "qa" => Option("gs://bike-trip/meta")
      case "prod" => Option("gs://bike-trip/meta")
      case _ => None
        throw new Exception("Stage not found. Please define the stage: dev, qa or prod")
    }
  )

  val outputUniqueUserPath: ScallopOption[String] = opt[String](
    name = "output.uniqueUser.path",
    descr = "path of the output unique user data",
    required = false,
    default = Option("gs://bike-trip/output/unique-user")
  )

  val outputPath: ScallopOption[String] = opt[String](
    name = "output.path",
    descr = "path of the output results",
    required = false,
    default = stage() match {
      case "dev" => Option("gs://bike-trip/output")
      case "qa" => Option("gs://bike-trip/output")
      case "prod" => Option("gs://bike-trip/output")
      case _ => None
        throw new Exception("Stage not found. Please define the stage: dev, qa or prod")
    }
  )

  val stage: ScallopOption[String] = opt[String](
    name = "stage",
    descr = "the stage of the environment: dev, qa or prod",
    required = false,
    default = Option("dev")
  )

  val selectColsFromResourceConf: ScallopOption[String] = opt[String](
    name = "selectcols.conf.file",
    descr = "column names to be selected, specified in the defaults.conf file",
    required = false,
    default = Option("defaults") // the name of the conf file (i.e. defaults.conf)
  )

  verify()
}
