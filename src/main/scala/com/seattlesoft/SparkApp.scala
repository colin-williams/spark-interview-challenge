package com.seattlesoft
import functions._
import constants._

object SparkApp {
  def main(args: Array[String]) {
    val spark = SparkSessionWrapper.spark
    val csvDF = spark.read.option("delimiter", " ").schema(constants.schema).csv(CSV_READ_PATH)
    val topNSourceRanking = topN(10, csvDF, spark, Source)
    topNSourceRanking.show()
    val topNRequestRanking = topN(10, csvDF, spark, Request)
    topNRequestRanking.show()
    topNSourceRanking.write.json(JSON_SOURCE_WRITE_PATH)
    topNRequestRanking.write.csv(JSON_REQUEST_WRITE_PATH)
    spark.close()
  }
}
