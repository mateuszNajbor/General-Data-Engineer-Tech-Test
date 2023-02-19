package tech.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source
import scala.reflect.io.Directory

class OddValuesAlgorithmTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Test app")
    .master("local[*]")
    .getOrCreate()

  val outputDirSmall = "src/test/resources/output_small"
  val outputDirBig = "src/test/resources/output_big"

  val outputDirectories: List[String] = List(outputDirSmall, outputDirBig)

  override def beforeAll(): Unit = {
    outputDirectories.foreach { dirName =>
      val dir = new Directory(new File(dirName))
      dir.createDirectory()
    }
  }

  test("Test small input") {
    val expectedResult = List(List(0, 9), List(430, 64), List(278, 0), List(451, 94), List(373, 157), List(205, 49))
    val inputDir = "src/test/resources/input_small"

    OddValuesAlgorithm.filterValuesOccurringOddNumberOfTimes(inputDir, outputDirSmall, spark)

    val result = getDataFromOutputFiles(outputDirSmall)

    result.size shouldBe expectedResult.size
    result should contain allElementsOf expectedResult
  }

  test("Test bigger input") {
    val expectedResult = List(List(3454, 16), List(454, 186), List(9200, 38), List(5420, 51), List(3106, 26), List(16044, 67), List(11490, 37), List(15652, 199), List(2670, 70), List(1994, 59), List(80, 48), List(8306, 88), List(10412, 146), List(14172, 178), List(12932, 78), List(2952, 175), List(19766, 117), List(7534, 67), List(13712, 187), List(3860, 62), List(10550, 74), List(9926, 177), List(8502, 191), List(2736, 67), List(5298, 40), List(11520, 67), List(3662, 190), List(3950, 122), List(7292, 0), List(2346, 162), List(4646, 23), List(632, 194), List(10508, 23), List(3226, 66), List(1806, 78), List(3804, 107), List(14706, 113), List(3872, 47), List(7150, 54), List(8234, 135), List(16726, 126), List(17226, 18), List(16358, 92), List(4364, 61), List(13640, 58), List(8262, 17), List(5472, 192), List(4654, 75), List(13402, 142), List(3428, 98), List(2628, 193), List(96, 20), List(5534, 195), List(10636, 194), List(2916, 23), List(3412, 1), List(18470, 184), List(13558, 164), List(12716, 24), List(9318, 184), List(5278, 0), List(14900, 184), List(1098, 169), List(5476, 106), List(14152, 52), List(7168, 101), List(15324, 114), List(12454, 171), List(5524, 141), List(3046, 93), List(12466, 199), List(17942, 2), List(10202, 148), List(14180, 153), List(16440, 147), List(11680, 174), List(10386, 52), List(760, 18), List(10646, 45), List(3254, 125), List(1192, 157), List(13136, 177), List(10298, 152), List(5352, 138), List(6572, 97), List(17230, 181), List(19768, 190), List(3482, 73), List(2868, 82), List(12112, 125), List(7488, 173), List(5116, 39), List(18764, 11), List(19536, 38), List(9824, 145), List(7676, 190), List(10782, 163), List(9538, 55), List(17506, 114), List(11164, 36), List(3930, 113), List(16978, 45), List(6580, 50), List(3694, 122), List(8294, 131), List(10398, 26), List(19460, 179), List(5629, 25), List(17767, 129), List(3863, 137), List(14797, 192), List(11607, 121), List(325, 114), List(5301, 198), List(16727, 49), List(11083, 176), List(5745, 3), List(4015, 166), List(11343, 44), List(12263, 2), List(15319, 15), List(14805, 166), List(3781, 173), List(12485, 180), List(14583, 139), List(13797, 122), List(9791, 97), List(12701, 147), List(4973, 14), List(8677, 80), List(11743, 137), List(16769, 141), List(10307, 121), List(5771, 32), List(9755, 126), List(3593, 122), List(19731, 71), List(14867, 15), List(7021, 32), List(1807, 123), List(9683, 97), List(18741, 185), List(9049, 12), List(15897, 24), List(8895, 55), List(19237, 16), List(18347, 67), List(11967, 133), List(4643, 118), List(9833, 31), List(13673, 63), List(4097, 185), List(1899, 165), List(11211, 176), List(3575, 50), List(1781, 133), List(13337, 5), List(14555, 121), List(18659, 124), List(16411, 39), List(5137, 131), List(12203, 12), List(8083, 103), List(6503, 96), List(12455, 137), List(8159, 186), List(16741, 118), List(2449, 192), List(15005, 34), List(4455, 34), List(8053, 185), List(6989, 197), List(19677, 121), List(16785, 47), List(13181, 82), List(15649, 59), List(14801, 160), List(1109, 145), List(4009, 22), List(19005, 70), List(14973, 179), List(16675, 163), List(13227, 0), List(13449, 0), List(9423, 29), List(14627, 111), List(18471, 74), List(5475, 75), List(14847, 146), List(16265, 9), List(209, 32), List(2579, 92), List(8821, 127), List(1851, 58), List(9267, 64))
    val inputDir = "src/test/resources/input_big"

    OddValuesAlgorithm.filterValuesOccurringOddNumberOfTimes(inputDir, outputDirBig, spark)

    val result = getDataFromOutputFiles(outputDirBig)

    result.size shouldBe expectedResult.size
    result should contain allElementsOf expectedResult
  }

  private def getDataFromOutputFiles(outputDir: String) = {
    val dir = new Directory(new File(outputDir))
    dir.list.toList.flatMap { fileName =>
      val source = Source.fromFile(fileName.toString)
      val lines = source.getLines().toList
      source.close()
      lines
    }.map(_.split("\t").map(_.toInt).toList)
  }

  override def afterAll(): Unit = {
    outputDirectories.foreach { dirName =>
      val dir = new Directory(new File(dirName))
      dir.list.foreach(fileName =>
        new File(fileName.toString).delete()
      )
    }
  }
}