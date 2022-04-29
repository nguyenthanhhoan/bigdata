package org.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

object Util {
  def uuid = java.util.UUID.randomUUID.toString
  def localToGMT() = {

    val utc = LocalDateTime.now(ZoneOffset.UTC)
    Timestamp.valueOf(utc)

  }

  def mesageSchema(spark: SparkSession) = {

    import spark.implicits._

    val message = new StructType()
      .add($"messageid".string)
      .add($"timestamp".timestamp)
      .add($"plot".string)
      .add($"state".string)

    message
  }
}
