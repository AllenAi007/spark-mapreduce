package com.ai.spark.csv

import org.scalatest._

/**
  * CsvFunction Test Cases
  */
class CsvFunctionsSpec extends FlatSpec with Matchers {

  import CsvFunctions._

  "A CsvImplicit" should "convert a People to csv String" in {
    People(1, "Allen AI", 32, "A Java Scala Developer").csv should be("1,Allen AI,32,A Java Scala Developer")
  }

  "A CsvImplicit" should "convert a People to csv String using | " in {
    People(1, "Allen AI", 32, "A Java Scala Developer").csv("|") should be("1|Allen AI|32|A Java Scala Developer")
  }

  "A CsvImplicit" should "convert a People AI\"HUA to \"AI\"\"HUA\" " in {
    People(1, "AI\"HUA", 32, "A Java Scala Developer").csv should be("1,\"AI\"\"HUA\",32,A Java Scala Developer")
  }

  "A CsvImplicit" should "convert a People AI,HUA to \"AI,HUA\" " in {
    People(1, "AI\"HUA", 32, "A Java Scala Developer").csv should be("1,\"AI\"\"HUA\",32,A Java Scala Developer")
  }

  "A CsvImplicit" should "convert a People AI\nHUA to \"AI\nHUA\" " in {
    People(1, "AI\nHUA", 32, "A Java Scala Developer").csv should be("1,\"AI\nHUA\",32,A Java Scala Developer")
  }

  "A CsvImplicit" should "convert a People with name null to empty " in {
    People(1, null, 32, "A Java Scala Developer").csv should be("1,,32,A Java Scala Developer")
  }

}

case class People(id: Int, name: String, age: Int, comment: String)