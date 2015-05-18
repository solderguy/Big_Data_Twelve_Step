package jsoper.com.pair

/**
 * @author John Soper
 * @revision 1  May 17, 2015
 * 
 *  ScalaTest routines for ClusterProcessing module
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
class ClusProSuite extends FunSuite with BeforeAndAfter {
  import ClusterProcessing._

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
      clusPoints += "0" -> "3.0,7.0"
      clusPoints += "0" -> "3.0,9.0"
      clusPoints += "1" -> "12.0,44.0"
      clusPoints += "1" -> "16.0,46.0"
      clusPoints += "2" -> "63.0,37.0"
      clusPoints += "2" -> "63.0,35.0"
      clusPoints += "3" -> "88.0,37.0"
      clusPoints += "3" -> "92.0,36.0"
      clusPoints += "4" -> "83.0,94.0"
      clusPoints += "4" -> "23.0,100.0"
      clusPoints += "5" -> "41.0,37.0"
      clusPoints += "5" -> "45.0,41.0"
      clusPoints += "6" -> "1.0,150.0"
      clusPoints += "6" -> "2.0,155.0"
      Points = sc.parallelize(clusPoints)
      clusMeans = scala.collection.mutable.MutableList[(String, Double)]()
      clusMeans += "0x" -> 4.0
      clusMeans += "0y" -> 5.0
      clusMeans += "1x" -> 9.4
      clusMeans += "1y" -> 9.6
      centroids = sc.parallelize(clusMeans)

      clusMeansFull = scala.collection.mutable.MutableList[(String, Double)]()
      clusMeansFull += "0x" -> 6.5
      clusMeansFull += "0y" -> 9.4
      clusMeansFull += "1x" -> 9.4
      clusMeansFull += "1y" -> 9.6
      clusMeansFull += "2x" -> 4.0
      clusMeansFull += "2y" -> 8.9
      clusMeansFull += "3x" -> 7.4
      clusMeansFull += "3y" -> 3.8
      clusMeansFull += "4x" -> 4.0
      clusMeansFull += "4y" -> 3.8
      clusMeansFull += "5x" -> 8.4
      clusMeansFull += "5y" -> 3.7
      clusMeansFull += "6x" -> 2.4
      clusMeansFull += "6y" -> 3.7
      centroidsFull = sc.parallelize(clusMeansFull)
      notRunYet = false
    }
  }

  test("XcoordToDoubleTest") {
    assert(ClusterProcessing.convXCoordDouble("1,3") === 1)
  }

  test("YcoordToDoubleTest") {
    assert(ClusterProcessing.convYCoordDouble("1,3") === 3)
  }

  test("getXCentroidVal") {
    var x = ClusterProcessing.getXCentroidVal("0", centroids)
    assert(x === 4.0, 0.01)

    x = ClusterProcessing.getXCentroidVal("1", centroids)
    assert(x === 9.4, 0.01)
  }

  test("getYCentroidVal") {
    var y = ClusterProcessing.getYCentroidVal("0", centroids)
    assert(y === 5.0, 0.01)

    y = ClusterProcessing.getYCentroidVal("1", centroids)
    assert(y === 9.6, 0.01)
  }

  test("getMeanTest") {
    var mean = ClusterProcessing.getMean(Points, "5", "x")
    assert(mean === 43.0, 0.01)

    mean = ClusterProcessing.getMean(Points, "6", "x")
    assert(mean === 1.5, 0.01)

    mean = ClusterProcessing.getMean(Points, "5", "y")
    assert(mean === 39.0, 0.01)

    mean = ClusterProcessing.getMean(Points, "6", "y")
    assert(mean === 152.5, 0.01)
  }

  test("calcCentroidsTest") {
    var centroids = calcCentroids(sc, Points)
    var arr = centroids.lookup("0x").toArray
    assert(arr(0) === 3.0, 0.01)

    arr = centroids.lookup("0y").toArray
    assert(arr(0) === 8.0, 0.01)

    arr = centroids.lookup("1x").toArray
    assert(arr(0) === 14.0, 0.01)

    arr = centroids.lookup("1y").toArray
    assert(arr(0) === 45.0, 0.01)

    arr = centroids.lookup("2x").toArray
    assert(arr(0) === 63.0, 0.01)

    arr = centroids.lookup("2y").toArray
    assert(arr(0) === 36.0, 0.01)

    arr = centroids.lookup("3x").toArray
    assert(arr(0) === 90.0, 0.01)

    arr = centroids.lookup("3y").toArray
    assert(arr(0) === 36.5, 0.01)

    arr = centroids.lookup("4x").toArray
    assert(arr(0) === 53.0, 0.01)

    arr = centroids.lookup("4y").toArray
    assert(arr(0) === 97.0, 0.01)

    arr = centroids.lookup("5x").toArray
    assert(arr(0) === 43.0, 0.01)

    arr = centroids.lookup("5y").toArray
    assert(arr(0) === 39.0, 0.01)

    arr = centroids.lookup("6x").toArray
    assert(arr(0) === 1.5, 0.01)

    arr = centroids.lookup("6y").toArray
    assert(arr(0) === 152.5, 0.01)
  }

  test("sortXAxisTest") {
    val finalOrder = ListBuffer[String]()
    val scratchpad = ListBuffer[String]("1", "2", "0")
    sortXAxis(scratchpad, finalOrder, centroidsFull)

    assert(finalOrder(0) === "2")
    assert(finalOrder(1) === "0")
    assert(finalOrder(2) === "1")
    assert(finalOrder.length === 4) // + 1 for space
    assert(scratchpad.length === 0)
  }

  test("sortYAxisTest") {
    val remaining = ListBuffer[String]("1", "2", "0", "3", "5", "6", "4")
    val finalOrder = sortYAxis(remaining, centroidsFull)
     
    assert(finalOrder(0) === "2")
    assert(finalOrder(1) === "0")
    assert(finalOrder(2) === "1")
    assert(finalOrder(3) === " ")
    assert(finalOrder(4) === "6")
    assert(finalOrder(5) === "4")
    assert(finalOrder(6) === "3")
    assert(finalOrder(7) === "5")
    assert(finalOrder.length === 8) // + 1 for space
  
  }
  

}