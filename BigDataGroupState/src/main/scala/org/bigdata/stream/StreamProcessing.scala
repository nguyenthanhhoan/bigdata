package org.bigdata.stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger}
import org.bigdata.utils.{Util,Message}

  object StreamProcessing {

    /**
     * operation system information
     */
    @transient val sys = System.getProperty("os.name")

    /**
     * application name
     */
    val appName = getClass.getCanonicalName

    /**
     * logger
     */
//    @transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass.getCanonicalName)

    def main(args: Array[String]) {

      /**
       * init kafka broker and topic
       */

      val kafkaBrokers = "13.212.15.4:9092" //config("kafka-brokers")
      val kafkaTopic = "metromind-raw" //config("topic")

      /**
       * init state management module
       */
      val stateMgmt = new StateMgmt()

      /**
       * get or create sparkSession
       */
      val spark = if (sys.startsWith("Mac")) {
        SparkSession
          .builder
          .appName(appName)
          .master("local[8]")
          .getOrCreate()
      } else {
        SparkSession
          .builder
          .appName(appName)
          .getOrCreate()
      }

      import spark.implicits._

      /**
       * setup spark configuration
       */
      spark.conf.set("spark.sql.shuffle.partitions", 8)
      spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
      spark.sparkContext.setLogLevel("ERROR")

      /**
       * read event stream from Kafka
       */
      val events = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", false)
        .load()
        .selectExpr( /**"timestamp",**/ "CAST(key AS STRING)", "CAST(value AS STRING)")
//        .select($"key", $"value".alias("value"))

      /**
       * schema to convert JSON string into a StructType
       */
      val schema = Util.mesageSchema(spark)

      /**
       * transform event stream from JSON string into a StructType
       */
      val splot = events.select(from_json($"value", schema).alias("json"))
        .select("json.*").as[Message]

      val spotParkedQuery = splot
        .groupByKey(x => x.plot.toString())
        .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(stateMgmt.spotTimeCount)
        .filter(x => x.parking)
        .writeStream
        .trigger(Trigger.ProcessingTime("60 seconds"))
        .format("console")
        .option("checkpointLocation", "hdfs://localhost:9000/Users/Storage/checkpoint")
        .outputMode("update")

      /**
       * start each of the streaming query
       */
      spotParkedQuery.start()

      /**
       * Wait until any of the queries on the associated SQLContext has terminated
       */
      spark.streams.awaitAnyTermination()

      spark.stop()

    }
}
