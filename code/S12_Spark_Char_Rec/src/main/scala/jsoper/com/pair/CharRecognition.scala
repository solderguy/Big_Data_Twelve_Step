package jsoper.com.pair

/**
 * @author John Soper
 * @revision 1  May 17, 2015
 * 
 * This is a support module for ClusterProcessing, it handles the 
 * more abstract activities
 */

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

object CharRecognition {

  def decodeAll(sortedOutput: RDD[(String, String)],
                finalOrder: ListBuffer[String]): ListBuffer[String] = {
    val finalLetters = ListBuffer[String]()
    for (cluster <- finalOrder) {
      finalLetters += fetchNextChar(cluster.toString, sortedOutput)
    }
    finalLetters
  }

  def fetchNextChar(cluster: String,
                    sortedOutput: RDD[(String, String)]): String = {
    var result = ""
    if (cluster.equals(" ")) // the space character between words 
      result = " "
    else {
      val calcs = calcGridProperties(sortedOutput, cluster)
      val symX = calcs._1
      val symY = calcs._2
      val distRatio = calcs._3
      //println(f"symX: $symX%.2f symY: $symY%.2f distRatio: $distRatio%.2f")
      val decodedChar = identifyChar(symX, symY, distRatio)
      result = decodedChar
    }
    result
  }

  def calcGridProperties(in: RDD[(String, String)],
                         clusNum: String): (Double, Double, Double) = {
    val singleCluster = in.filter(t => t._1 == clusNum)
    val len = 15 //using a 15x15 bitmap to parse each character
    val grid = createAndFillArray(singleCluster, len)
    
    val symmetryX = calcXSymmetry(grid)
    val symmetryY = calcYSymmetry(grid)
    val distanceRatio = calcDistanceRatio(grid)
    (symmetryX, symmetryY, distanceRatio)
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
  

  def createAndFillArray(singleCluster: RDD[(String, String)],
                         len: Int): Array[Array[Int]] = {
    //singleCluster.foreach(println)
    //println("len: " + len)
    val clusXY = singleCluster.map { case (clus, xy) => (xy) }
    val clusDoublesX = clusXY.map(convXCoordDouble)
    val clusDoublesY = clusXY.map(convYCoordDouble)

    // move point data from RDD into local arrays of type double
    val pointsX = clusDoublesX.collect
    val pointsY = clusDoublesY.collect

    // calculate shifts so bitmap characters are centered in grid array
    val shiftX = calcShift(pointsX)
    val shiftY = calcShift(pointsY)

    val grid = Array.fill(len, len)(0)
    for (i <- 0 until pointsX.length) {
      var xVal = (pointsX(i) - shiftX).toInt
      var yVal = len - 1 - (pointsY(i) - shiftY).toInt
      grid(yVal)(xVal) = 1
    }
    grid
  }

  def calcShift(points: Array[Double]): Double = {
    var min: Double = Double.MaxValue
    var max: Double = Double.MinValue
    for (pt <- points) {
      if (pt < min) min = pt
      if (pt > max) max = pt
    }
    val shift = min + (max - min) / 2 - 7
    shift
  }

  def calcXSymmetry(grid: Array[Array[Int]]): Double = {
    val len = grid.length
    val half = len / 2 + 1
    var count = 0.0
    var sum = 0.0
    for (x <- 0 until len) {
      for (y <- 0 until half) {
        sum += Math.abs(grid(x)(y) - grid(x)(len - 1 - y))
        if (grid(x)(y) == 1) count += 1
      }
    }
    val symmetryX = sum / count
    symmetryX
  }

  def calcYSymmetry(grid: Array[Array[Int]]): Double = {
    val len = grid.length
    val half = len / 2 + 1
    var count = 0.0
    var sum = 0.0
    for (x <- 0 until half) {
      for (y <- 0 until len) {
        sum += Math.abs(grid(x)(y) - grid(len - 1 - x)(y))
        if (grid(x)(y) == 1) count += 1
      }
    }
    val symmetryY = sum / count
    symmetryY
  }

  def calcDistanceRatio(grid: Array[Array[Int]]): Double = {
    val len = grid.length
    val half = len / 2 + 1
    var closeCount = 0
    var farCount = 0
    for (x <- 0 until len) {
      for (y <- 0 until len) {
        if (grid(x)(y) == 1) {
          if (Math.abs(x - half) <= 2 && Math.abs(y - half) <= 2)
            closeCount += 1
          else
            farCount += 1
        }
      }
    }
    if (farCount < 1) farCount = 1 // avoid divide by zero
    val distanceRatio = closeCount.toDouble / (closeCount + farCount)
    distanceRatio
  }

  def identifyChar(symX: Double,
                   symY: Double,
                   distRatio: Double): String = {
    var encoding = 0
    if (symX < 0.3) encoding += 1 // lower is more symmetric
    if (symY < 0.3) encoding += 10
    if (distRatio < 0.1) encoding += 100 // most points far from center
    if (distRatio > 0.4) encoding += 1000 // most points close to center (ex lowercase)

    var c = ""
    encoding match {
      case 0       => c = "G"
      case 1       => c = "T"
      case 10      => c = "B"
      case 11      => c = "I"
      case 100     => c = "G"
      case 101     => c = "T"
      case 110     => c = "D"
      case 111     => c = "I"
      case 1000    => c = "A"
      case 1001    => c = "T"
      case 1010    => c = "B"
      case 1011    => c = "I"
      case unknown => println("Unexpected case: " + unknown.toString)
    }
    c
  }
}
