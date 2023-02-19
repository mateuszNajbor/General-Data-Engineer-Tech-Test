package tech.test

import org.apache.spark.sql.SparkSession

object TechTestBoot extends App {

  if (args.length < 2) {
    throw new IllegalArgumentException("At least 2 arguments are needed!")
  }

  //TODO add support for S3
  val inputDir = args(0)
  val outputDir = args(1)

  val spark = SparkSession
    .builder()
    .appName("Tech test app")
    .master("local[*]")
    .getOrCreate()

  OddValuesAlgorithm.filterValuesOccurringOddNumberOfTimes(inputDir, outputDir, spark)

}
