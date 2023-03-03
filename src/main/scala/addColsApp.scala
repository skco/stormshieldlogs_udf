package stormshieldLogs

import org.apache.spark.sql.api.java.UDF1

class addColsApp extends UDF1[String,Map[String,String]] {

    var cols = Map( "" -> "")  // empty Map for use in UDF mapColumns"

    override def call(row: String):Map[String, String] = {
      val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)" // match ;= outside double quotes ""
      val pairs = row.split(pattern).grouped(2) // split and group to (key, value)
      val result = cols ++ pairs.map { case Array(k, v) => k -> v }.toMap //insert values
      result
    }
  }
