package tech.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable
import scala.util.Using

object OddValuesAlgorithm {

  def filterValuesOccurringOddNumberOfTimes(input: String, output: String, spark: SparkSession): Unit = {
    val initialRDD = readAndCleanData(input, spark)

    val resultRDD = initialRDD.groupByKey.mapValues(algorithmWithGroupBy)

    saveAsTSVFile(output, resultRDD)
    //saveAsTextWithTSVFormat(output, resultRDD)
    //saveAsCSVWithTSVFormat(output, resultRDD, spark)
  }

  private def readAndCleanData(input: String, spark: SparkSession): RDD[(Int, Int)] = {
    spark.sparkContext
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
  }

  /*
  * Time complexity:
  * - groupBy is 0(n)
  * - find is O(lg(n)) in normal case, in worse case is 0(n). Size of iterable depends on the collection used. Probably O(lg(n)) in normal case, in worse case is 0(n)
  *
  * Space complexity:
  * - groupBy is O(n)
  * */
  private def algorithmWithGroupBy(values: Iterable[Int]) = {
    values.groupBy(identity).find((el: (Int, Iterable[Int])) => el._2.size % 2 == 1)
      .getOrElse(throw new IllegalArgumentException(s"Wrong input. Not found odd occurrences of value"))
      ._1
  }

  /*
  * Complexity:
  * Time complexity:
  * - iteration with adding elements to Map is 0(n)
  * - find is O(lg(n)) in normal case. In worse case is 0(n).
  *
  * Space complexity:
  * - iteration with adding elements to Map is 0(n)
  * */
  private def algorithmWithHashMap(values: Iterable[Int]) = {
    val map = mutable.Map.empty[Int, Int]

    values.foreach { value =>
      val currentOccurrences = map.getOrElse(value, 0)
      map(value) = currentOccurrences + 1
    }

    map
      .find(el => el._2 % 2 == 1)
      .getOrElse(throw new IllegalArgumentException(s"Wrong input. Not found odd occurrences of value"))
      ._1
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
