package com.seattlesoft

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import constants._

object functions {

  /**
    * decodes the formatted timestamp for a StringType Column, resulting in a timestamp column in UTC by applying the
    * zone information. see preconditions required from test case
    *
    * @param col - a column of StringType following expected preconditions
    * @return - a column in UTC time, assuming preconditions
    */
  def decodeTimestamp(col: Column) = {
    to_timestamp(col, "'['dd/MMM/yyyy:H:m:s XX']'")
  }

  /**
    * topN returns a dense ranking of for the specified rankColumnName. See test or dense_rank documentation for more
    * @param n - the rank of values to return for each date in the dataframe
    * @param df - the DataFrame conforming to the expected input schema
    * @param spark - the SparkSession used for computation
    * @param rankColumnName - the TopNColumnName to rank against
    * @return - a DataFrame returning the topN values for each date
    */
  def topN(
      n: Int,
      df: DataFrame,
      spark: SparkSession,
      rankColumnName: TopNColumnName
  ): DataFrame = {
    import spark.implicits._
    spark.sql(SPARK_SET_UTC)

    assert(
      df.schema == constants.schema,
      "DataFrame schema does not conform to expected schema",
    )

    val nextFrame = df
      .withColumn(
        "datetime",
        concat(col("timestamp1"), lit(" "), col("timestamp2"))
      )
      .drop("hyphen1", "hyphen2", "timestamp1", "timestamp2")
      .withColumn("datetime", functions.decodeTimestamp(col("datetime")))
      .withColumn("date", to_date(col("datetime")))
      .groupBy(col("date"), col(rankColumnName.toString))
      .count()

    val windowSpec = Window.partitionBy(col("date")).orderBy('count desc)
    nextFrame
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .filter('dense_rank < n + 1)
  }

  def sparkReadSchema(spark: SparkSession): DataFrameReader = {
    spark.read.option("delimiter", " ").schema(constants.schema)
  }
}
