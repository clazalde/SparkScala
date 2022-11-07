/*package com.SparkScala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, format_number, initcap, sum}

object Scala_Spark {
  def main(args : Array[String]): Unit ={
    val spark:SparkSession = SparkSession.builder().master("local[3]").appName("SparkByExample").getOrCreate()
    Logger.getRootLogger.setLevel(Level.INFO)

    val df2 = spark.read.option("delimiter",",").option("header","true").csv("C:/Users/Consultant/Documents/SharedUbuntu/tbl2.csv")
    val df1 = spark.read.option("delimiter",",").option("header","true").csv("C:/Users/Consultant/Documents/SharedUbuntu/tbl1.csv")
    df1.unionAll(df2).groupBy(initcap(col("person")) as "person").agg(format_number(sum("eranings"),2) as "TotalEarnings").orderBy("person").show()

  }
}*/
