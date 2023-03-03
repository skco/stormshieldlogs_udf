package stormshieldLogs


import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{call_udf, col, lit, when}


import org.apache.spark.sql.types.DataTypes

import scala.collection.immutable.Map


object stormshieldLogs {


  var fullCols:Map[String,String] = Map( // empty Map     for use in UDF mapColumns
    "" -> "")

  def RawTextStoreParquet(spark:SparkSession,loadPath:String,savePath:String): Dataset[Row] = {
    //load multiple text files, save to parquet format and return dataframe
    val dfLogs: Dataset[Row] = spark.read.text(loadPath)
    dfLogs.repartition(1).write.mode("overwrite").parquet(savePath)

    dfLogs
  }

  def GetAndStoreUniqueCols(spark: SparkSession, df: Dataset[Row]):Map[String,String] = {
    //get all unique column sets from dataset within string  "field1=value1 field2=value2"
    import spark.implicits._
    val result = df
      .withColumn("value", regexp_replace(col("value"), "\"(.*?)\"", ""))
      .withColumn("value", regexp_replace(col("value"), "(?<==).*?(?=( ([a-z])|$))", ""))
      .withColumn("value", regexp_replace(col("value"), "=", ""))
      .select(split(col("value"), " ").as("IdsArray")).distinct()
      .withColumn("IdsArray", explode($"IdsArray")).distinct()
      //result.write.mode("overwrite").text("uniqueIDs.txt")
      //create colName -> NULL Map
      val headers = result.select("IdsArray").collect.toList
      headers.map(t => t.getString(0) -> "NULL").toMap // create empty columns map
  }

  var cols: Map[String, String] = Map( // empty Map for use in UDF mapColumns
    "" -> "",
  )

val mapCols = (row: String) => {
  val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)" // match ;= outside double quotes ""
  val pairs = row.split(pattern).grouped(2) // split and group to (key, value)
  val result = cols ++ pairs.map { case Array(k, v) => k -> v }.toMap //insert values
  result
  }:Map[String,String]

val saveIntermediateResults:Boolean = false

def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("StormShieldLogs")
      .master("local[*]")
      .getOrCreate()

  import spark.implicits._

        // log types (directory)
        //alarm.auth,connections,filterstat,monitor,plugin,system,web

        val logDir = "auth"
        val logDirPath = s"E:/logsALL/${logDir}/*.log"
        val logsDF = spark.read.text(logDirPath)

        if(saveIntermediateResults) {logsDF.write.mode("overwrite").parquet(s"${logDir}.parquet")}

        fullCols = GetAndStoreUniqueCols(spark,logsDF) // store into global variable

        //-------------------------------------------------------------------------------------------------------------------------------------------------
        // tak działa gdy do register.udf przekazuja sie bezpośrednio funkcje zwracajaca Map[String,String]
        val mapColUDF = udf(mapCols) // mapCols funkcja zdeklarowana lokalnie
        spark.udf.register("mapColUDF",mapColUDF)


         //-------------------------------------------------------------------------------------------------------------------------------------------------
        //to nie działa bo w przypadku gdy drugim parametrem udf.register jest class to w trzecim trzeba wpisac zwracany typ
        //nia działa Map[String,String] ani java.util.Map które rzekomo jest klasa pierwotną


        //val mapColUDF: addColsApp = new addColsApp()
        //mapColUDF.cols = fullCols
        //spark.udf.register("mapColUDF",mapColUDF,?????) // jaki typ przekazać dla Map[String,String]?

        //-------------------------------------------------------------------------------------------------------------------------------------------------


        //logsDF.show(false)

        val result = logsDF.withColumn("value", call_udf("mapColUDF", col("value")))

        result.show(false)

        val keysDF = result.select(explode(map_keys($"value"))).distinct()           // extract keys to DF
        val keys = keysDF.collect().map(f => f.get(0))                               // extract keys from DF to Map
        val keyCols = keys.map(f => col("value").getItem(f).as(f.toString)) // create Array of columns from Map

        val finalResult = result.select(col("value") +: keyCols: _*).drop("value")

        if(saveIntermediateResults) {finalResult.write.mode("overwrite").parquet(s"${logDir}_final.parquet")}

        val usersCount:Int = finalResult.select("user").distinct().count().toInt

        finalResult.select("user").distinct().sort("user").show( usersCount,false)

        println(usersCount)
        // logsDF.show(truncate = false)
        //finalResult.show(truncate = false)
  }
}

