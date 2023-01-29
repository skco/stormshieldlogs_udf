package interest


import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, StructField, StructType, TimestampType}

object interestCalc {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("interest")
      .master("local")
      .getOrCreate()

    val InterestCapitalizationWithContributionUDF: InterestCapitalizationWithContributionUDF = new InterestCapitalizationWithContributionUDF()
    spark.udf.register("InterestCapitalizationWithContributionUDF", InterestCapitalizationWithContributionUDF, DataTypes.DoubleType)

    val InterestCapitalizationUDF: InterestCapitalizationUDF = new InterestCapitalizationUDF()
    spark.udf.register("InterestCapitalizationUDF", InterestCapitalizationUDF, DataTypes.DoubleType)

   var peopleDF = spark.read
     .option("header", "true")
     .csv("money_saving.csv")

    peopleDF.show()
    peopleDF.printSchema()

    val peopleWithMoneYearlyContributionDF: Dataset[Row] = peopleDF
      .withColumn("money", col("money").cast(DataTypes.DoubleType))
      .withColumn("interest", col("interest").cast(DataTypes.DoubleType))
      .withColumn("10yearsContribution", call_udf("InterestCapitalizationWithContributionUDF", col("money"), lit(10), col("interest"),lit(1000)))
      .withColumn("20yearsContribution", call_udf("InterestCapitalizationWithContributionUDF", col("money"), lit(20), col("interest"),lit(1000)))
      .withColumn("40yearsContribution", call_udf("InterestCapitalizationWithContributionUDF", col("money"), lit(40), col("interest"),lit(1000)))
      .withColumn("60yearsContribution", call_udf("InterestCapitalizationWithContributionUDF", col("money"), lit(60), col("interest"),lit(1000)))

    val peopleWithMoneyNoContributionDF: Dataset[Row] = peopleDF
      .withColumn("money", col("money").cast(DataTypes.DoubleType))
      .withColumn("interest", col("interest").cast(DataTypes.DoubleType))
      .withColumn("10years", call_udf("interestCapitalizationUDF", col("money"), lit(10), col("interest")))
      .withColumn("20years", call_udf("interestCapitalizationUDF", col("money"), lit(20), col("interest")))
      .withColumn("40years", call_udf("interestCapitalizationUDF", col("money"), lit(40), col("interest")))
      .withColumn("60years", call_udf("interestCapitalizationUDF", col("money"), lit(60), col("interest")))

    peopleWithMoneYearlyContributionDF.show()
    peopleWithMoneyNoContributionDF.show()

  }
}
