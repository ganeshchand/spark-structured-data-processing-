package com.gc.spark.sql.example.util
import java.sql.Timestamp
/**
 * Created by ganeshchand on 7/22/15.
 */
object EventSampleData {
  private val random = scala.util.Random
  //DateTime, when passed to spark is not supported. Only java.sql.Timestamp is supported
  def generateSampleData(n: Int = 5) = {
    (1 to 5).map {
      x => (new Timestamp(new java.util.Date().getTime), random.nextDouble())
    }.toList
  }
}
