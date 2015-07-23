package com.gc.spark.sql.example
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by ganeshchand on 7/22/15.
 */

object Person {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(getClass.getCanonicalName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // This is required for toDF() to work

    val peopleRDD = sc.textFile("src/main/resources/people.txt")
      .map(_.split(" "))
      .map(x => Person(x(0), x(1).trim.toInt))

    val peopleDF = peopleRDD.toDF()

    peopleDF.registerTempTable("people")

    val rowCount = sqlContext.sql("select count(*) AS cnt from people").collect().head.getLong(0)

    println(s"Total Rows: $rowCount")


  }

}
