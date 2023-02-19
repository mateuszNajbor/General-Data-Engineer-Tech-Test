package tech.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, FileWriter}
import scala.util.Using

object OddValuesAlgorithm {

  def filterValuesOccurringOddNumberOfTimes(input: String, output: String, spark: SparkSession): Unit = {
    val initialRDD = spark.sparkContext
      .wholeTextFiles(input)
      .flatMap(_._2.split("\n").drop(1)
        .map(_.split("[\t,]", -1))
        .map(el =>
          (
            if (el(0).isEmpty) 0 else el(0).toInt,
            if (el(1).isEmpty) 0 else el(1).toInt
          )
        )
        .iterator
      )

    val resultRDD = initialRDD.groupByKey.mapValues { values =>
      values.groupBy(identity).filter(_._2.size % 2 == 1).head._1
    }

    saveAsTSVFile(output, resultRDD)
    //saveAsTextWithTSVFormat(output, resultRDD)
    //saveAsCSVWithTSVFormat(output, resultRDD, spark)
  }

  //TODO maybe I should create output dir if it not exists. Not specified.
  private def saveAsTSVFile(output: String, resultRDD: RDD[(Int, Int)]): Unit = {
    resultRDD
      .map(el => el._1 + "\t" + el._2)
      .mapPartitionsWithIndex((idx, itr) => itr.map(x => (x, idx)))
      .foreachPartition { iterator =>
        if (iterator.hasNext) {
          val list = iterator.toList
          val index = list.head._2
          val stringToWrite = list.map(_._1).mkString("\n")
          Using(new BufferedWriter(new FileWriter(s"$output/output_$index.tsv"))) { bufferedWriter =>
            bufferedWriter.write(stringToWrite) //TODO add error if output dir does not exist
          }
        }
      }
  }

  //Additional implementations of saving files if saving csv file or text file with tsv format is enough
  private def saveAsTextWithTSVFormat(output: String, resultRDD: RDD[(Int, Int)]): Unit = {
    resultRDD
      .map(el => el._1 + "\t" + el._2)
      .saveAsTextFile(output)
  }

  private def saveAsCSVWithTSVFormat(output: String, resultRDD: RDD[(Int, Int)], spark: SparkSession): Unit = {
    spark.createDataFrame(resultRDD)
      .write
      .option("delimiter", "\t")
      .csv(output)
  }
}
