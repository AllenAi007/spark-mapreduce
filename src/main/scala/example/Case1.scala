package example

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Map Reduce case one
  *
  * how to run.
  *
  * cd spark_home
  ./bin/spark-submit --master local[2] --class example.Case1 /Users/aihua/work/spark-map-reduce/target/scala-2.11/spark-map-reduce_2.11-0.1.0-SNAPSHOT.jar
  *
  *
  */
object Case1 extends App with Logging{

  // refer resources/spark_case1.csv
  val input = "/Users/aihua/work/spark-map-reduce/src/test/resources/spark_case1.csv"

  // delete the folder first if exits, other wise, it will show error
  val output = "/Users/aihua/work/spark-map-reduce/src/test/resources/output"

  val spark = SparkSession.builder().appName("AIHUA-MAPREDUCE-CASE1").getOrCreate

  val dataFrame = spark.read.option("header", true).csv(input);

  // since the row is immutable object, so we need to map into class
  dataFrame.rdd.map(r=>
      (
        r.getAs[String]("masterId"),
        MyObject(rowType = r.getAs[String]("h"),
          masterId = r.getAs[String]("masterId"),
          branchId = r.getAs[String]("branchId"),
          branchName = r.getAs[String]("branchName"))
      )
  ).reduceByKey((o1, o2) => {
    o1.copy(branchId = s"${o1.branchId}||${o2.branchId}",
      branchName = s"${o1.branchName}||${o2.branchName}")
  })
  //map to csv
    .map(tuple2 => Case1Functions.toCsv(tuple2._2))
    .saveAsTextFile(output)

}

object Case1Functions {

  def toCsv(myobj: MyObject): String = s"${myobj.rowType},${myobj.masterId},${myobj.branchId},${myobj.branchName}"

}


case class MyObject(rowType: String,masterId: String,branchId: String,branchName: String)
