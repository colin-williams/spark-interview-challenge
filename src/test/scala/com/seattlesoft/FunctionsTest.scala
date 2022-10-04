package com.seattlesoft

import org.scalatest.FunSpec
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Dataset
import com.seattlesoft.constants._
import com.seattlesoft.functions._

class FunctionsTest
    extends FunSpec
    with SparkSessionWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe(
    "decodeTimestamp: Given the expected timestamp format, preconditions, and Dataframe"
  ) {

    it(
      "decodeTimestamp returns the DataFrame with datetime column as TimestampType with timestamp Row in UTC"
    ) {

      // preconditions required to have UTC properly set spark configuration and sql set timezone
      val spark = SparkSessionWrapper.spark
      spark.sql(SPARK_SET_UTC)
      val df = Seq("[28/Jul/1995:13:32:22 -0400]").toDF("datetime")
      val expected = Seq("1995-07-28 17:32:22")
        .toDF("datetime")
        .withColumn("datetime", to_timestamp(col("datetime")))
      val transformed =
        df.withColumn("datetime", functions.decodeTimestamp(col("datetime")))
      assertSmallDataFrameEquality(expected, transformed)
    }
  }

  describe(
    "topN: Given the expected data format, spark session, when invoked using the Source constant"
  ) {
    it("TopN provides the expected result set using a dense ranking") {
      val spark = SparkSessionWrapper.spark
      import spark.implicits._

      val data: Dataset[String] = spark.sparkContext
        .parallelize(
          """
          |topRanking - - [28/Jul/1995:13:32:22 -0400] "GET / HTTP/1.0" 200 234
          |topRanking - - [28/Jul/1995:13:32:22 -0400] "GET /shuttle/technology/images/srb_mod_compare_1-small.gif HTTP/1.0" 200 36902
          |topRanking - - [28/Jul/1995:13:32:22 -0400] "GET /images/shuttle-patch-logo.gif HTTP/1.0" 200 891
          |secondRanking - - [28/Jul/1995:13:32:22 -0400] "GET /shuttle/technology/images/srb_mod_compare_3-small.gif HTTP/1.0" 200 55666
          |secondRanking - - [28/Jul/1995:13:32:22 -0400] "GET /shuttle/technology/images/srb_mod_compare_6-small.gif HTTP/1.0" 200 28219
          |thirdRanking - - [28/Jul/1995:13:32:23 -0400] "GET /images/KSC-logosmall.gif HTTP/1.0" 200 1204
          |nextDayTopRanking - - [29/Jul/1995:13:32:23 -0400] "GET /shuttle/missions/missions.html HTTP/1.0" 200 8677
          |nextDayTopRanking - - [29/Jul/1995:13:32:23 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 200 5866
          |nextDayTopRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayTopRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDaySecondRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDaySecondRanking - - [29/Jul/1995:13:32:26 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDaySecondRanking - - [29/Jul/1995:13:32:27 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayThirdRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayThirdRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayAnotherThirdRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayAnotherThirdRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
          |nextDayFourthRanking - - [29/Jul/1995:13:32:25 -0400] "GET /shuttle/missions/sts-74/sts-74-patch-small.gif HTTP/1.0" 200 5494
      """.stripMargin.linesIterator.toList
        )
        .toDS()

      val dataFrame = sparkReadSchema(spark).csv(data)

      val output = functions.topN(5, dataFrame, spark, Source)

      val expectedData: Dataset[String] = spark.sparkContext
        .parallelize("""
          |1995-07-29 nextDayTopRanking 4 1
          |1995-07-29 nextDaySecondRanking 3 2
          |1995-07-29 nextDayThirdRanking 2 3
          |1995-07-29 nextDayAnotherThirdRanking 2 3
          |1995-07-29 nextDayFourthRanking 1 4
          |1995-07-28 topRanking 3 1
          |1995-07-28 secondRanking 2 2
          |1995-07-28 thirdRanking 1 3
        """.stripMargin.linesIterator.toList)
        .toDS()

      val expectedDF = spark.read
        .option("delimiter", " ")
        .schema(sourceTestSchema)
        .csv(expectedData)
        .withColumn("date", to_date(col("date")))

      assertSmallDataFrameEquality(output, expectedDF, orderedComparison = false)
    }
  }

  describe(
    "topN: Given the expected data format, spark session, when invoked using the Request constant"
  ) {
    it("TopN provides the expected result set using a dense ranking") {
      val spark = SparkSessionWrapper.spark
      import spark.implicits._

      val data: Dataset[String] = spark.sparkContext
        .parallelize(
          """
          |host1 - - [28/Jul/1995:13:32:22 -0400] "GET /top resource" 200 234
          |host2 - - [28/Jul/1995:13:32:22 -0400] "GET /top resource" 200 36902
          |host3 - - [28/Jul/1995:13:32:22 -0400] "GET /top resource" 200 891
          |host4 - - [28/Jul/1995:13:32:22 -0400] "GET /second place resource" 200 55666
          |host5 - - [28/Jul/1995:13:32:22 -0400] "GET /second place resource" 200 28219
          |host6 - - [28/Jul/1995:13:32:23 -0400] "GET /third place resource" 200 1204
          |host3 - - [29/Jul/1995:13:32:25 -0400] "GET /first place next day resource" 200 5494
      """.stripMargin.linesIterator.toList
        )
        .toDS()

      val dataFrame = sparkReadSchema(spark).csv(data)

      val output = functions.topN(5, dataFrame, spark, Request)

      val expectedData: Dataset[String] = spark.sparkContext
        .parallelize("""
          |1995-07-29 "GET /first place next day resource" 1 1
          |1995-07-28 "GET /top resource" 3 1
          |1995-07-28 "GET /second place resource" 2 2
          |1995-07-28 "GET /third place resource" 1 3
        """.stripMargin.linesIterator.toList)
        .toDS()

      val expectedDF = spark.read
        .option("delimiter", " ")
        .schema(requestTestSchema)
        .csv(expectedData)
        .withColumn("date", to_date(col("date")))

      assertSmallDataFrameEquality(output, expectedDF, orderedComparison = false)
    }
  }
}
