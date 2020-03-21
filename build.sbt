name := "bikeTrip"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.0",
    "org.apache.spark" %% "spark-sql" % "2.3.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.rogach" %% "scallop" % "3.1.2",
    "com.typesafe" % "config" % "1.3.1",
    "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.14.0" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}