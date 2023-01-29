package interest

import org.apache.spark.sql.api.java.{UDF2, UDF3, UDF4}
import math.pow

class InterestCapitalizationUDF extends UDF3[Double, Int, Double, Double]{
  override def call(startCapital: Double, yearsAmount: Int, interest: Double): Double = {
    "%.2f".format(startCapital*pow((1 + interest.toDouble/100), yearsAmount)).toDouble
  }
}


class InterestCapitalizationWithContributionUDF extends UDF4[Double, Int, Double, Int, Double]{
  override def call(startCapital: Double, yearsAmount: Int, interest: Double,contribution:Int): Double = {
    "%.2f".format(startCapital*pow((1 + interest.toDouble/100), yearsAmount)).toDouble

    var P:Double   = startCapital   //start deposit
    var PMT:Double = contribution // regular contribution
    var r:Double   = interest.toDouble   //interest rate
    var n:Double   = 1   // contribution  frequency 1 = yearly
    var t:Double   = yearsAmount  //  years


    val A: Double = P * math.pow(1 + ((r / 100) / n), n * t)
    val S: Double = PMT * ((math.pow(1 + ((r / 100) / n), n * t) - 1) / (r / 100)) * 12

    "%.2f".format(A+S).toDouble


    //


  }
}
