package jsoper.com.pair

/*
Error when running from sbt on command line, but safe to ignore.  See
http://stackoverflow.com/questions/28362341/error-utils-uncaught-exception-in-thread-sparklistenerbus
for details
*/
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
//import grizzled.slf4j.Logging
import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets
import java.io.IOException

//object ClusterProcessing with Logging {
object ClusterProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext("local", "HW2", conf)

    val file = sc.sequenceFile("outS11", classOf[Text], classOf[Text])
    val map = file.map { case (k, v) => (k.toString(), v.toString()) }
    val sortedOutput = map.sortByKey(true)
    println("Number of data points: " + sortedOutput.count);

    // Create Centroid RDD for the 7 clusters
    val centroids = calcCentroids(sc, sortedOutput)
    centroids foreach { case (s, n) => println(s + " " + n) }

    // Determine word and character order
    val initialRandomOrder = ListBuffer[String]("4", "2", "5", "0", "1", "6", "3")
    val finalOrder = sortYAxis(initialRandomOrder, centroids)

    // Character recognition
    val finalLetters = CharRecognition.decodeAll(sortedOutput, finalOrder)

    val finalLetterString = finalLetters.mkString("")
    println("\nDecoded Value: " + finalLetterString + "\n")
    Files.deleteIfExists(Paths.get("myfile.txt"))
    Files.write(Paths.get("outS12.txt"),
      finalLetterString.getBytes(StandardCharsets.UTF_8))

//    try {
//      sc.stop()
//    } catch {
//      case ex: IOException => {
//        println("Caught IO Exception")
//        }
//      case _: Exception => {
//        println("Caught some other Exception")
//      }
//    }

  }

  def sortYAxis(remaining: ListBuffer[String],
                centroids: RDD[(String, Double)]): ListBuffer[String] = {
    val finalOrder = ListBuffer[String]()
    val scratchpad = ListBuffer[String]()
    var maxY = -1E9
    var y = -1E9

    while (!remaining.isEmpty) {
      // find the top-most centroid (ie maximum y value)
      maxY = -1E9
      remaining.foreach { cluster =>
        y = getYCentroidVal(cluster, centroids)
        maxY = Math.max(maxY, y)
      }

      // collect other clusters that are in same line (same y-level)
      remaining.foreach { cluster =>
        y = getYCentroidVal(cluster, centroids)
        val diff = Math.abs(y - maxY) / maxY
        if (diff < 0.20) { // 20%
          scratchpad += cluster
          remaining -= cluster
        }
      }
      sortXAxis(scratchpad, finalOrder, centroids) // called for each line
    }
    finalOrder.remove(finalOrder.size - 1) // delete blank space at end
    finalOrder
  }

  def sortXAxis(scratchpad: ListBuffer[String],
                finalOrder: ListBuffer[String],
                centroids: RDD[(String, Double)]): Unit = {
    var minX = 1E9
    var x = 1E9

    while (!scratchpad.isEmpty) {
      // find the left-most centroid (ie minimum x value)
      minX = 1E9
      scratchpad.foreach { pt =>
        x = getXCentroidVal(pt, centroids)
        minX = Math.min(minX, x)
      }

      // loop back through to cluster and remove it
      scratchpad.foreach { pt =>
        x = getXCentroidVal(pt, centroids)
        val diff = Math.abs(x - minX) / minX
        if (diff < 0.01) { // 1% because it will be the same value
          finalOrder += pt // each cluster (bitmap character) is in proper order
          scratchpad -= pt // no break statement, it's a functional language
        }
      }
    }
    finalOrder += " " // put space at end of each word
  }

  def getXCentroidVal(clusNum: String, centroids: RDD[(String, Double)]): Double = {
    val arr = centroids.lookup(clusNum + "x").toArray
    arr(0)
  }

  def getYCentroidVal(clusNum: String, centroids: RDD[(String, Double)]): Double = {
    val arr = centroids.lookup(clusNum + "y").toArray
    arr(0)
  }

  def calcCentroids(sc: SparkContext, in: RDD[(String, String)]): RDD[(String, Double)] = {
    val clusMeans = scala.collection.mutable.MutableList[(String, Double)]()
    clusMeans += "0x" -> getMean(in, "0", "x")
    clusMeans += "0y" -> getMean(in, "0", "y")
    clusMeans += "1x" -> getMean(in, "1", "x")
    clusMeans += "1y" -> getMean(in, "1", "y")
    clusMeans += "2x" -> getMean(in, "2", "x")
    clusMeans += "2y" -> getMean(in, "2", "y")
    clusMeans += "3x" -> getMean(in, "3", "x")
    clusMeans += "3y" -> getMean(in, "3", "y")
    clusMeans += "4x" -> getMean(in, "4", "x")
    clusMeans += "4y" -> getMean(in, "4", "y")
    clusMeans += "5x" -> getMean(in, "5", "x")
    clusMeans += "5y" -> getMean(in, "5", "y")
    clusMeans += "6x" -> getMean(in, "6", "x")
    clusMeans += "6y" -> getMean(in, "6", "y")
    sc.parallelize(clusMeans)
  }

  def getMean(in: RDD[(String, String)], clusN: String, axis: String): Double = {
    val singleCluster = in.filter(t => t._1 == clusN)
    val clusXY = singleCluster.map { case (clus, xy) => (xy) }

    val clusDoubles =
      if (axis.equals("x"))
        clusXY.map(ClusterProcessing.convXCoordDouble)
      else
        clusXY.map(ClusterProcessing.convYCoordDouble)
    val avg = clusDoubles.map { case (pt) => (pt) }.mean()
    avg
  }

  def convXCoordDouble(in: String): Double = {
    val tokens = in.split(",").map(_.trim)
    var x: Double = tokens(0).toDouble
    (x)
  }

  def convYCoordDouble(in: String): Double = {
    val tokens = in.split(",").map(_.trim)
    var y: Double = tokens(1).toDouble
    (y)
  }
}
