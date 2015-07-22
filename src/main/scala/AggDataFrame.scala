/**
 * Created by ganeshchand on 7/22/15.
 */

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._

import scala.tools.nsc.classpath.FileUtils

object AggDataFrame {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("DataFrame Aggregation Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val inputFilePath = getClass.getResource("sales.csv").getPath.replace("%20", " ")
    println(s"Reading csv file from: $inputFilePath")
    val df = sQLContext.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true")
      .load(inputFilePath)


    df.printSchema()

    df.select(
      df("transactionId"),
      df("customerId"),
      df("itemId"),
      df("amountPaid")
    ).filter(df("customerId") equalTo ("1")).show()

    val outputDirectory = "/tmp/customerID1"
    if(org.apache.commons.io.FileUtils.deleteQuietly(new File(outputDirectory))){
      println(s"$outputDirectory already exists. It has been deleted.")
    }
    df
      .select("transactionId", "customerId", "itemId", "amountPaid")
      .filter(df("customerId") equalTo ("1"))
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/tmp/customerId1")


    val transCount = df
    .select("transactionId","customerId")
    .filter(df("customerId") === "1")
    .collect()
    .size

    println(s"Total Transaction for Customer Id 1 :$transCount")


    val query =
      """
        |CREATE TEMPORARY TABLE customer
        |USING com.databricks.spark.csv
        |OPTIONS (path
      """.stripMargin.replace("\n", " ")
  }

}
