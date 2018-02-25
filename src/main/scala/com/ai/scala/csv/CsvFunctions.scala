package com.ai.scala.csv

/**
  * Common CSV functions
  */
object CsvFunctions {

  implicit class CsvImplicit(product: Product) {

    /**
      * To CSV String
      *
      * @param delimiter
      * @return
      */
    def csv(implicit delimiter: String = ","): String = product.productIterator.map(map2String).mkString(delimiter)

    /**
      * map any value to string
      *
      * @param any
      * @param delimiter
      * @return
      */
    private[CsvImplicit] def map2String(any: Any)(implicit delimiter: String): String = {
      if (any == None || any == null) {
        ""
      }
      else if (any.isInstanceOf[String])
        csvString(any.asInstanceOf[String])
      else {
        any.toString
      }
    }


    /**
      * convert a string to csv format,
      *
      * @param input
      * @return 1. null => ""
      *         2. test"double => "test""double"
      *         3. new
      *         line => "new
      *         line"
      */
    private[CsvImplicit] def csvString(input: String)(implicit delimiter: String): String = {
      if (input.contains("\"")) "\"" + input.replaceAll("\"", "\"\"") + "\""
      else if (input.contains("\n") || input.contains(delimiter)) "\"" + input + "\""
      else input
    }

  }


}
