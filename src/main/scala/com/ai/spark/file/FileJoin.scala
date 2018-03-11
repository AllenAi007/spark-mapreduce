package com.ai.spark.file

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
  * Given student.csv, address.csv,
  * Then Generate an output with students and their address
  *
  * ./bin/spark-submit --master local[2] --class com.ai.spark.file.One2One /Users/aihua/work/spark-mapreduce/target/scala-2.11/spark-mapreduce_2.11-0.1.0-SNAPSHOT.jar
  */
object One2One extends App with Logging {

  val output = "/Users/aihua/work/spark-mapreduce/target/output"

  // input file
  val studentFile = "/Users/aihua/work/spark-mapreduce/src/test/resources/filejoin/one2one/student.csv"

  val addressFile = "/Users/aihua/work/spark-mapreduce/src/test/resources/filejoin/one2one/address.csv"

  val spark = SparkSession.builder().appName("FILE_ONE2ONE").getOrCreate

  val student = spark.read.option("header", true).csv(studentFile)

  val address = spark.read.option("header", true).csv(addressFile)

  val result = student.join(address, student("address_id") === address("id"), "inner").cache

  logInfo(s"Total record after join-> ${result.count}")

  logInfo(s"Start writing to output $output")

  result.select(student("id"),
    student("name"),
    student("age"),
    address("country"),
    address("province"),
    address("city"),
    address("street"),
    address("postcode"))
    .write.option("header", true)
    .csv(output)

}

/**
  * Given student.csv, cause.csv, student_cause.csv,
  * Then Generate an output with students and their causes
  */
object One2Many extends App with Logging {
  // input file
  val input = args.apply(0).toString


}

/**
  * Given student.csv, cause.csv, student_cause.csv,
  * Then Generate an output with students and their causes
  */
object Many2Many extends App with Logging {
  // input file
  val input = args.apply(0).toString


}



