package com.gc.spark.sql.example

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.gc.spark.sql.example.util.EventSampleData._
import java.util.Calendar
import java.text.SimpleDateFormat

/**
 * Created by ganeshchand on 7/22/15.
 *
 * This example demonstrates:
 * Define your own schema for Spark SQL and apply it to create dataframe
 * How to perform aggregation
 *
 * Aggregation: Aggregate events. an event is a tuple consisting of timestamp and decimal value
 *
 */
object CustomSchemaDateTime {

  case class Event(year: Int, dayOfYear: Int, hour: Int, createdDate: java.sql.Timestamp, value: Double)

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(getClass.getCanonicalName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val eventRDD = sc.parallelize(generateSampleData())

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val calendar = Calendar.getInstance()

    val eventDF = eventRDD.map { x =>
      calendar.setTime(x._1)
      Event(
        calendar.get(Calendar.YEAR),
        calendar.get(Calendar.DAY_OF_YEAR),
        calendar.get(Calendar.HOUR),
        x._1,
        x._2.toDouble
      )

    }.toDF().registerTempTable("events")

    sqlContext.cacheTable("events")


    // you can also use this
    //     sqlContext.sql("CACHE TABLE events")
    println("Printing all records from events tables")
    sqlContext.sql("SELECT * FROM events").show()

    println("Printing aggregates")
    //Imp: Alias name cannot be same as the the aggregate function. example count(*) as count returns error
    sqlContext.sql(
      """
        |SELECT year,
        |       COUNT(*) AS cnt,
        |       AVG(value) AS average,
        |       MIN(value) AS minimum,
        |       MAX(value) AS maximum
        |FROM events
        |GROUP BY year, dayOfYear
        |ORDER BY year, dayOfYear
      """.stripMargin).show()

  }

}
