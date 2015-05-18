package jsoper.com.pair

/**
 * @author John Soper
 * @revision 1  May 17, 2015
 * 
 * ScalaTest routines for CharRecognition module
 */

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.SparkContext.rddToPairRDDFunctions

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CharRecogSuite extends FunSuite with BeforeAndAfter {
  import CharRecognition._

  var notRunYet = true;
  var conf: SparkConf = _
  var sc: SparkContext = _
  var clusPoints: scala.collection.mutable.MutableList[(String, String)] = _
  var clusMeans: scala.collection.mutable.MutableList[(String, Double)] = _
  var centroids: RDD[(String, Double)] = _
  var Points: RDD[(String, String)] = _
  var clusMeansFull: scala.collection.mutable.MutableList[(String, Double)] = _
  var centroidsFull: RDD[(String, Double)] = _

  before {
    if (notRunYet) { // beforeAll was giving problems, so fake it
      conf = new SparkConf().setAppName("Twelve Step Unit Testing")
      sc = new SparkContext("local", "HW2", conf)

      clusPoints = scala.collection.mutable.MutableList[(String, String)]()
      clusPoints += "1" -> "55.0,72.0"
      clusPoints += "1" -> "56.0,72.0"
      clusPoints += "1" -> "57.0,73.0"
      clusPoints += "1" -> "58.0,72.0"
      clusPoints += "1" -> "59.0,74.0"
      clusPoints += "1" -> "60.0,75.0"
      clusPoints += "1" -> "61.0,72.0"

      clusPoints += "1" -> "62.0,72.0"
      clusPoints += "1" -> "63.0,72.0"
      clusPoints += "1" -> "59.0,73.0"
      clusPoints += "1" -> "59.0,74.0"
      clusPoints += "1" -> "59.0,75.0"
      clusPoints += "1" -> "59.0,76.0"
      clusPoints += "1" -> "59.0,77.0"
      clusPoints += "1" -> "59.0,78.0"
      clusPoints += "1" -> "59.0,79.0"
      clusPoints += "1" -> "59.0,80.0"
      clusPoints += "1" -> "59.0,81.0"
      clusPoints += "1" -> "55.0,82.0"
      clusPoints += "1" -> "56.0,82.0"
      clusPoints += "1" -> "57.0,82.0"
      clusPoints += "1" -> "58.0,82.0"
      clusPoints += "1" -> "59.0,82.0"
      clusPoints += "1" -> "60.0,82.0"
      clusPoints += "1" -> "61.0,82.0"
      clusPoints += "1" -> "62.0,82.0"
      clusPoints += "1" -> "63.0,82.0"

      Points = sc.parallelize(clusPoints)

      notRunYet = false
    }
  }

  test("identifyCharTest") {
    var str = identifyChar(0.2, 0.2, 0.05)
    assert(str === "I")
    str = identifyChar(0.2, 0.2, 0.25)
    assert(str === "I")
    str = identifyChar(0.4, 0.2, 0.05)
    assert(str === "D")
    str = identifyChar(0.4, 0.2, 0.25)
    assert(str === "B")

    str = identifyChar(0.4, 0.4, 0.05)
    assert(str === "G")
    str = identifyChar(0.4, 0.4, 0.25)
    assert(str === "G")
    str = identifyChar(0.2, 0.4, 0.05)
    assert(str === "T")
    str = identifyChar(0.2, 0.4, 0.25)
    assert(str === "T")

    str = identifyChar(0.2, 0.4, 0.5)
    assert(str === "T")
    str = identifyChar(0.4, 0.4, 0.5)
    assert(str === "A")
    str = identifyChar(0.4, 0.2, 0.5)
    assert(str === "B")
    str = identifyChar(0.2, 0.2, 0.5)
    assert(str === "I")
  }

  test("XcoordToDoubleTest") {
    assert(convXCoordDouble("1,3") === 1)
  }

  test("YcoordToDoubleTest") {
    assert(ClusterProcessing.convYCoordDouble("1,3") === 3)
  }

  test("calcXSymmetryTest") {
    var grid = Array.fill[Int](16, 16)(0)
    grid(0)(0) = 1; grid(0)(15) = 1
    grid(1)(0) = 1; grid(1)(15) = 1
    grid(2)(0) = 1
    grid(3)(0) = 1; grid(3)(15) = 1
    grid(4)(0) = 1
    grid(5)(0) = 1
    grid(10)(5) = 1; grid(10)(10) = 1
    //println(grid.deep.mkString("\n"))

    val sym = calcXSymmetry(grid)
    val str = sym.toString.substring(0, 5) // can't easily assert on doubles
    assert(str === "0.428")
  }

  test("calcYSymmetryTest") {
    var grid = Array.fill[Int](16, 16)(0)
    grid(0)(7) = 1; grid(15)(7) = 1
    grid(1)(7) = 1; grid(14)(7) = 1
    grid(2)(7) = 1
    grid(3)(8) = 1; grid(12)(7) = 1
    grid(0)(8) = 1
    grid(1)(8) = 1
    grid(2)(2) = 1; grid(13)(2) = 1
    //println(grid.deep.mkString("\n"))

    val sym = calcYSymmetry(grid)
    val str = sym.toString.substring(0, 5)
    assert(str === "0.714")
  }

  test("calcDistanceRatioTest") {
    var grid = Array.fill[Int](16, 16)(0)
    grid(0)(7) = 1
    grid(15)(7) = 1
    grid(1)(7) = 1
    grid(14)(7) = 1
    grid(7)(7) = 1
    grid(7)(8) = 1
    grid(8)(7) = 1
    grid(8)(8) = 1
    grid(2)(2) = 1
    grid(13)(2) = 1
    grid(12)(7) = 1
    //println(grid.deep.mkString("\n"))

    val dr = calcDistanceRatio(grid)
    val str = dr.toString.substring(0, 5)
    assert(str === "0.363")
  }

  test("calcShiftTest") {
    var points = Array.fill[Double](16)(0)
    points(10) = 130
    points(11) = 128
    points(12) = 127
    points(14) = 132

    val shift = calcShift(points)
    val str = shift.toString.substring(0, 4)
    assert(str === "59.0")
  }

  test("createAndFillArrayTest") {
    val grid = createAndFillArray(Points, 15)
    //println(grid.deep.mkString("\n"))

    assert(grid(2)(3) === 1)
    assert(grid(2)(4) === 1)
    assert(grid(2)(5) === 1)
    assert(grid(2)(6) === 1)
    assert(grid(2)(7) === 1)
    assert(grid(2)(8) === 1)
    assert(grid(2)(9) === 1)
    assert(grid(2)(7) === 1)
    assert(grid(3)(7) === 1)
    assert(grid(4)(7) === 1)
    assert(grid(5)(7) === 1)
    assert(grid(6)(7) === 1)
    assert(grid(7)(7) === 1)
    assert(grid(8)(7) === 1)
    assert(grid(9)(7) === 1)
    assert(grid(10)(7) === 1)
    assert(grid(11)(7) === 1)
    assert(grid(12)(3) === 1)
    assert(grid(12)(4) === 1)
    assert(grid(12)(6) === 1)
    assert(grid(12)(9) === 1)
    assert(grid(12)(10) === 1)
    assert(grid(12)(11) === 1)
    assert(grid(11)(5) === 1)
    assert(grid(11)(4) === 0) // verify not all high
  }

  test("calcGridPropertiesTest") {
    val (symmetryX, symmetryY, distanceRatio) = calcGridProperties(Points, "1")
    //          println("symmX: "+ symmetryX)
    //          println("symmY: "+ symmetryY)
    //          println("DS: "+ distanceRatio)

    var str = symmetryX.toString.substring(0, 5)
    assert(str === "0.222")

    str = symmetryY.toString.substring(0, 5)
    assert(str === "0.357")

    str = distanceRatio.toString.substring(0, 5)
    assert(str === "0.230")
  }

  test("fetchNextCharTest") {
    val str = fetchNextChar("1", Points)
    assert(str === "T")
  }
}