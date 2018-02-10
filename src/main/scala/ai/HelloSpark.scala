package ai

import org.apache.spark.sql.SparkSession

/**
  * Sample spark job, where i copy all content from below spark page,
  *
  * 1. how many 'Spark' works are there in total
  * 2. which line has max number of 'Spark' words
  * 3. how many lines contains 'Spark'
  *
  */
object HelloSpark extends App {

  import HelloSparkFunc._

  val file = "/Users/aihua/share/inbound/hello-spark.txt"
  val spark = SparkSession.builder.appName("AI-HELLO-SPARK").getOrCreate
  import spark.implicits._

  val data = spark.read.text(file).cache

  val keyWord = "Spark"
  // map to number of Spark words
  val map = data.map(line => getNumberOfWord(keyWord, line.mkString)).cache
  // 1. total number
  val totalWords = map.reduce((a, b) => a + b)
  // 2. which line has max number of Spark words
  val maxNumberOfLine = map.reduce((a, b) => if(a > b) a else b)
  // 3. how many lines contains 'Spark'
  val lineNumberWithSpark = data.filter(line=> line.mkString.contains("Spark")).count

  println(s"1. how many 'Spark' works are there in total: $totalWords")
  println(s"2. which line has max number of 'Spark' words: $maxNumberOfLine")
  println(s"3. how many lines contains 'Spark': $lineNumberWithSpark")

}

object HelloSparkFunc {

  def getNumberOfWord(word: String, line: String): Int = {
    var result = 0
    line.split(" ").foreach(s => if (s.equals(word)) {
      result += 1
    })
    result
  }

}