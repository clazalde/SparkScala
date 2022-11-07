package com.SparkScala
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, weekofyear, year}


object snowf {
def main(args : Array[String]): Unit = {
//create session
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkDemo04")
    .getOrCreate()
// read votes_file
  val df = spark.read.json("C:/Users/Consultant/Documents/SharedUbuntu/Votes.json")
// provide connection options to snowflake
  val sfOptions = Map(
    "sfURL" -> "https://lbb45113.us-east-1.snowflakecomputing.com",
    "sfUser" -> "clazalde",
    "sfPassword" -> "Lemonade4..",
    "sfDatabase" -> "DATABASE",
    "sfSchema" -> "GENERALDATA",
    "sfWarehouse" -> "GEN",
    "sfRole" -> "accountadmin"
  )
// write into votes_table into snowflake ignoring duplicates
  df.write
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "Votes_table")//.option("query", "select id, name, preferences, created_at from big_data_table1")
    .mode(SaveMode.Ignore)
    .save()
// reading from votes_table from snowflake
  val dfs = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable", "Votes_table") //.option("query", "select id, name, preferences, created_at from big_data_table1")
    .load()
// creating data frame grouping weeks
  val dfweek = dfs.groupBy(year(col("CreationDate")).alias("year"), weekofyear(col("CreationDate")).alias("week")).agg(count(col("CreationDate")).alias("NumVotes"))
// create a tempview to query the data


  dfweek.createOrReplaceTempView("Votes_weeks")
// query the data to get the outliers +- 20% from average using window func
  val dfOut = spark.sql("select avg1.year, avg1.week, avg1.NumVotes " +
  "from (Select year, week, NumVotes, avg(NumVotes) over() as AverageVotes from Votes_weeks) avg1 left join " +
  "(Select year, week, NumVotes, avg(NumVotes) over() as AverageVotes from Votes_weeks) avg2 " +
  "on avg2.year = case when avg1.week = 1 then avg1.year -1 else avg1.year end " +
  "and avg2.week = case when avg1.week = 1 then 52 else avg1.week - 1 end " +
  "where (avg1.NumVotes > avg1.AverageVotes * 1.2 or avg1.NumVotes < avg1.AverageVotes * .8) " +
  "and not ((avg2.NumVotes > avg2.AverageVotes * 1.2 or avg2.NumVotes < avg2.AverageVotes * .8) or av2.NumVotes is null)")
// show the results

  val dfOut2 = spark.sql("select year, week, NumVotes " +
  "from (Select year, week, NumVotes, avg(NumVotes) over() as AverageVotes, lag(NumVotes,1,170) over(Order by year, week) as weekant from Votes_weeks) avg1 " +
  "where (NumVotes > AverageVotes * 1.2 or NumVotes < AverageVotes * .8) " +
  "and not (weekant > AverageVotes * 1.2 or weekant < AverageVotes * .8)")
  dfOut2.show()

  dfOut.write.csv("C:/Users/Consultant/Documents/SharedUbuntu/Votes_Out/")
}
}
