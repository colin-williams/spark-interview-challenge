package com.seattlesoft

import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

object constants {
  val schema: StructType = StructType(
    Array(
      StructField(Source.toString(), StringType, true),
      StructField("hyphen1", StringType, true),
      StructField("hyphen2", StringType, true),
      StructField("timestamp1", StringType, true),
      StructField("timestamp2", StringType, true),
      StructField(Request.toString(), StringType, true),
      StructField("response_code", IntegerType, true),
      StructField("bytes", IntegerType, true)
    )
  )

  val sourceTestSchema: StructType = StructType(
    Array(
      StructField("date", DateType, true),
      StructField("source", StringType, true),
      StructField("count", LongType, false),
      StructField("dense_rank", IntegerType, true)
    )
  )

  val requestTestSchema: StructType = StructType(
    Array(
      StructField("date", DateType, true),
      StructField("request", StringType, true),
      StructField("count", LongType, false),
      StructField("dense_rank", IntegerType, true)
    )
  )

  val SPARK_SET_UTC = "SET TIME ZONE 'UTC'"

  sealed trait TopNColumnName

  case object Source extends TopNColumnName {
    override def toString(): String = "source"
  }

  case object Request extends TopNColumnName {
    override def toString(): String = "request"
  }

  val CSV_READ_PATH = "/opt/spark/work-dir/NASA_access_log_Jul95.gz"
  val JSON_SOURCE_WRITE_PATH = "/opt/source/work-dir/topN_source.json"
  val JSON_REQUEST_WRITE_PATH= "/opt/source/work-dir/topN_request.json"

}
