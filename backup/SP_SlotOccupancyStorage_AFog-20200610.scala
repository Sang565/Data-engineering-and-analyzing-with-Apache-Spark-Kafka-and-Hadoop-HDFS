/**
 * ======================================================================================
 * - This code is used for the third paper -->
 *   --> continuing develop from the code in the second paper (submitted to TII journal on Oct, 31)
 *
 * - Use Spark-Kafka Integration + Structured Streaming
 * - Run this code to collect data from GW layer and process at S-Fog1 (LPC-01) cluster
 * - Functions:
 *   + Real-time receiving streaming slot occupancy (carpark occupancy) records after processing at S-Fog1 and S-Fog2, then storing them under CSV/JSON/Parquet format in HDFS storage system at this fog
 *     --> Run this code to collect data from S-Fog sub-tiers and process at A-Fog (HPC) cluster
 *   + Run another application for data analytics purpose
 *   + Be carefull with Processing-time and Event-time here!!!
 *     --> Process based on the event-time (but use the name of Processing-time) embedded in the carpark-occupancy records of corresponding carparks sent from S-Fog clusters
 * - Changes
 * 21/03/2020:
 *  + Step 2.3.1 and 2.3.2: change term: current-time --> processing-time ==> Be careful, see the explanation above
 *        --> using processing-time in the records (it is actually the event-time from carpark occupancy records) sent from S-Fog --> each CP will have each own event-time !!!
 *  + IMPORTANT: add .trigger() at write to HDFS --> giam phan manh with too many small files
 *  + Ignore: //.selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")  //SANG-IMPORTANT: 21/03/2020
 *  + .trigger(Trigger.ProcessingTime("2 minutes"))
 *
 * 27/03/2020:
 *  + Add 10 minutes Trigger
 *
 * 20/04/2020
 *  + Add more receiving streams for data generated in the past time --> make it compatiple with the new file in SFog-01
 *  /multiCarparkOccupancyStorage_JSON-2-past, /multiCarparkOccupancyStorage_JSON-3-past
 *  + Change directory name in HDFS: /multiCarparkOccupancyStorage_JSON_20200321 --> /multiCarparkOccupancyStorage_JSON
 * 20/04/2020-v2
 *  + Try 10 times faster in generating and collecting data --> but losing of stream records
 *       //.trigger(Trigger.ProcessingTime("10 minutes"))  //SANG-IMPORTANT: 20/04/2020
 *       .trigger(Trigger.ProcessingTime("1 minutes"))  //SANG-IMPORTANT: 20/04/2020-v2 --> 10 times faster for collecting past data
 *  + sp-slot-occupancy2: "10 times faster" version (6 seconds for updating data in real to work as 1 minute for SFog-01 --> change simulator,
 *                                                  and 1min to work as 10min Trigger at AFog)
 * 21/04/2020
 *  + sp-slot-occupancy2: Try with "5 times faster" version ("12 seconds" for updating data in real to work as 1 minute for SFog-01 --> change simulator,
 *                             and "2min" to work as 10min Trigger at AFog)
 * 22/04/2020
 *  + sp-slot-occupancy2: Try with "2 times faster" version ("30 seconds" for updating data in real to work as 1 minute for SFog-01 --> change simulator,
 *                             and "5min" to work as 10min Trigger at AFog)
 *    .trigger(Trigger.ProcessingTime("5 minutes"))
 *
 * 25/04/2020
 *  Return to the 5x faster version to synchronyse with the SFog-01 code
 *   .trigger(Trigger.ProcessingTime("2 minutes"))
 *
 * 29/05/2020
 *   Add 2 args for write_speed on HDFS storage 2 and topic 3 in order to get data in the corresponding time with generating_speed in SFog (generating_speed="12 seconds" = 5x faster --> write_speed="2 minutes" means "10 minutes" Trigger in writing data out) for simulation
 *   Or: in real speed ("10 minutes")
 *
 * 10/06/2020
 *   Change to use with afog-master and Spark 2.4.5 (build.sbt)
 * ========================================================
 * - NOTES:
 *   + Duoc ke thua tu project SP_SlotOccupancyStorageCSV_AFog in the 2nd paper, with changing the name of project by deleting CSV
 *
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.sql.streaming.Trigger //SANG: 23/10

object SP_SlotOccupancyStorage_AFog {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: SP_SlotZoneOccupancy_SFog2 <write_speed_2>, <write_speed_3>")
      System.exit(1)
    }

    //val generating_speed_2 = "12 seconds" //SANG: 29/05/2020 --> 5x faster
    val write_speed_2 = args(0)
    val write_speed_3 = args(1)
    //***************************************************************************************************
    //=== Step 1: Create a Spark session and variables: topic, kafka brokers =======
    val sparkMaster = "spark://afog-master:7077"
    //val checkpointLocation = "file:///home/pi/spark-applications/checkpoint" //SANG: use local storage on Spark's nodes --> copy the same files and folder to ALL nodes
    val checkpointLocation = "/checkpoint" //SANG: edit to HDFS path

    val kafkaBrokers_afog = "afog-kafka01:9092"
    val slotOccupancyInputTopic = "sp-slot-occupancy"
    val slotOccupancyInputTopic2 = "sp-slot-occupancy-2"  //20/04/2020
    val slotOccupancyInputTopic3 = "sp-slot-occupancy-3"  //20/04/2020

    val spark = SparkSession.builder()
      .appName("SP_SlotOccupancyStorage_AFog")
      .master(sparkMaster)
      .config("spark.sql.streaming.checkpointLocation", checkpointLocation) //SANG: for RPi
      .getOrCreate()

    import spark.implicits._
    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR) //SANG: only display ERROR, not display WARN

    //***************************************************************************************************
    //=== Step 2: Read streaming messages from Kafka topics, select desired data and parse on this data for processing =======
    //--- Step 2.1: Read and parse node mapping files (CSV file) --------------------

    //--- Step 2.2: Read streaming messages and extract desired elements (e.g. value) from Kafka topics -------
    //--- 2.2.1: Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic) //subscribe Kafka topic name: sp-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    // SANG-20/04/2020: add more streams collecting on slotOccupancyInputTopic-2 and -3
    val kafka2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic2) //subscribe Kafka topic name: sp-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    val kafka3 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic3) //subscribe Kafka topic name: sp-topic
      //.option("startingOffsets", "earliest")
      .option("startingOffsets", "latest")
      .load()

    //--- 2.2.2: Selecting desired data read from Kafka topic
    val kafkaData = kafka
      //.withColumn("Key", $"key".cast(StringType))	//SANG: add "Key" column from $"key" element
      //.withColumn("Topic", $"topic".cast(StringType))
      //.withColumn("Offset", $"offset".cast(LongType))
      //.withColumn("Partition", $"partition".cast(IntegerType))
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")
    val rawData = kafkaData.selectExpr("CAST(Value as STRING)")

    // SANG-20/04/2020: add more streams collecting on slotOccupancyInputTopic-2 and -3
    val kafkaData2 = kafka2
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      .select("Value", "Timestamp")
    val rawData2 = kafkaData2.selectExpr("CAST(Value as STRING)")

    val kafkaData3 = kafka3
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) //SANG: this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      .select("Value", "Timestamp")
    val rawData3 = kafkaData3.selectExpr("CAST(Value as STRING)")

    //--- Step 2.3: parse data for processing in the next stage  -------
    //--- 2.3.1: define a schema for parsing
    val dataStreamSchema = new StructType()
      .add("carparkID", StringType)
      //.add("current-time", StringType)  //SANG: 21/03/2020 --> current-time is the event-time in carpark-occupancy record sent from S-Fog
      .add("processing-time", StringType)  //SANG: 21/03/2020
      .add("slotOccupancy", StringType)

    //--- 20/04/2020
    val dataStreamSchema2 = new StructType()
      .add("carparkID", StringType)
      //.add("current-time", StringType)  //SANG: 21/03/2020 --> current-time is the event-time in carpark-occupancy record sent from S-Fog
      .add("event-time", StringType)  //SANG: 21/03/2020
      .add("slotOccupancy", StringType)

    //--- 2.3.2: parse data for doing processing in the next stage
    val multiCarparkOccupancyData = rawData
      .select(from_json(col("Value"), dataStreamSchema).alias("multiCarparkOccupancyData")) //SANG: Value from Kafka topic
      .select("multiCarparkOccupancyData.*") //SANG: 2 lines are OK, but want to test the line below
      .select($"processing-time", $"carparkID", $"slotOccupancy") //SANG: 21/03/2020

      //.withColumn("event-time", $"current-time".cast(TimestampType))	//SANG-IMPORTANT change: 21/03/2020
      .withColumn("processing-time", $"processing-time".cast(TimestampType))	//SANG-IMPORTANT change: 21/03/2020
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))

      //.select("event-time", "carparkID", "slotOccupancy") //SANG: 21/03/2020
      .select("processing-time", "carparkID", "slotOccupancy") //SANG: 21/03/2020

    println("\n=== multiCarparkOccupancyData schema ====")
    multiCarparkOccupancyData.printSchema()   //SANG: ko co dau () o sau Schema

    // SANG-20/04/2020: add more streams collecting on slotOccupancyInputTopic-2 and -3
    val multiCarparkOccupancyData2 = rawData2
      .select(from_json(col("Value"), dataStreamSchema2).alias("multiCarparkOccupancyData2")) //SANG: Value from Kafka topic
      .select("multiCarparkOccupancyData2.*") //SANG: 2 lines are OK, but want to test the line below
      .select($"event-time", $"carparkID", $"slotOccupancy") //SANG: 20/04/2020 --> change to event-time to match with timestamp in receiving records
      .withColumn("processing-time", $"event-time".cast(TimestampType))	//SANG-IMPORTANT 20/04/2020: change column name to match with data format with the current one (seen multiCarparkOccupancyData)
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))

      .select("processing-time", "carparkID", "slotOccupancy") //SANG: 21/03/2020

    println("\n=== multiCarparkOccupancyData2 schema ====")
    //multiCarparkOccupancyData2.printSchema()   //SANG: ko co dau () o sau Schema

    val multiCarparkOccupancyData3 = rawData3
      .select(from_json(col("Value"), dataStreamSchema2).alias("multiCarparkOccupancyData3")) //SANG: Value from Kafka topic
      .select("multiCarparkOccupancyData3.*") //SANG: 2 lines are OK, but want to test the line below
      .select($"event-time", $"carparkID", $"slotOccupancy") //SANG: 20/04/2020 --> change to event-time to match with timestamp in receiving records
      .withColumn("processing-time", $"event-time".cast(TimestampType))	//SANG-IMPORTANT 20/04/2020: change column name to match with data format with the current one (seen multiCarparkOccupancyData)
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))

      .select("processing-time", "carparkID", "slotOccupancy") //SANG: 21/03/2020

    println("\n=== multiCarparkOccupancyData3 schema ====")
    //multiCarparkOccupancyData3.printSchema()   //SANG: ko co dau () o sau Schema

    //==================================================================================
    //--- 4.2.1.3: output result --> Chu y .trigger(Trigger.ProcessingTime("1 minute"))
    multiCarparkOccupancyData
      .withWatermark("processing-time", "10 milliseconds") //SANG: unused
      //.selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")  //SANG-IMPORTANT: 21/03/2020
      //--- If has the above line, the record formate will be quite strange --> hardly to process
      // {"key":"1","value":"{\"processing-time\":\"2020-03-21T18:07:50.030+13:00\",\"carparkID\":\"1\",\"slotOccupancy\":151}"}
      //--- Without the line: {"processing-time":"2020-03-22T10:40:00.023+13:00","carparkID":"1","slotOccupancy":52}
      .writeStream
      .format("json")
      //.outputMode("complete")  //SANG: Ko support cho File Sink
      //.outputMode("update")   //SANG: Ko support cho File Sink
      .outputMode("append") //SANG: Voi CSV thi ko support updata, che do File Sink chi support Append mode without doing Aggregate task
      //https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#output-sinks
      .option("truncate", false) //SANG: 21/03/2020
      .option("checkpoint", "/checkpoint/")

      .option("path", "/multiCarparkOccupancyStorage_JSON-1/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      //.option("path", "/multiCarparkOccupancyStorage_JSON/")   //SANG: for production 20/04/2020

      //.trigger(Trigger.ProcessingTime("2 minutes"))  //SANG-IMPORTANT: 21/03/2020, giam phan manh (write to so mamy small files)
      .trigger(Trigger.ProcessingTime("10 minutes"))  //SANG-IMPORTANT: 21/03/2020, giam phan manh (write to so mamy small files)
      .start()
    //.awaitTermination()

    // SANG-20/04/2020: add more streams collecting on slotOccupancyInputTopic-2 and -3
    //val write_speed_2 = args(0)
    multiCarparkOccupancyData2
      .withWatermark("processing-time", "20 milliseconds") //SANG: unused
      .writeStream
      .format("json")
      .outputMode("append")
      .option("truncate", false) //SANG: 21/03/2020
      .option("checkpoint", "/checkpoint/")

      //.option("path", "/multiCarparkOccupancyStorage_JSON_20200321/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      .option("path", "/multiCarparkOccupancyStorage_JSON-2/")   //SANG: for production 20/04/2020

      //.trigger(Trigger.ProcessingTime("10 minutes"))  //SANG-IMPORTANT: 20/04/2020 --> normal speed
      //.trigger(Trigger.ProcessingTime("1 minutes"))  //SANG-IMPORTANT: 20/04/2020-v2 --> 10x faster for collecting past data
      //.trigger(Trigger.ProcessingTime("2 minutes"))  //SANG-IMPORTANT: 21/04/2020 --> 5x faster for collecting past data
      .trigger(Trigger.ProcessingTime(write_speed_2))
      //.trigger(Trigger.ProcessingTime("5 minutes"))  //SANG-IMPORTANT: 22/04/2020 --> 2x faster for collecting past data
      .start()
    //.awaitTermination()

    //val write_speed_3 = args(3)
    multiCarparkOccupancyData3
      .withWatermark("processing-time", "20 milliseconds") //SANG: unused
      .writeStream
      .format("json")
      .outputMode("append")
      .option("truncate", false) //SANG: 21/03/2020
      .option("checkpoint", "/checkpoint/")

      //.option("path", "/multiCarparkOccupancyStorage_JSON_20200321/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      .option("path", "/multiCarparkOccupancyStorage_JSON-3/")   //SANG: for production 20/04/2020

      //.trigger(Trigger.ProcessingTime("10 minutes"))  //SANG-IMPORTANT: 20/04/2020 --> normal speed
      //.trigger(Trigger.ProcessingTime("1 minutes"))  //SANG-IMPORTANT: 20/04/2020-v2 --> 10x faster for collecting past data
      //.trigger(Trigger.ProcessingTime("2 minutes"))  //SANG-IMPORTANT: 21/04/2020 --> 5x faster for collecting past data
      .trigger(Trigger.ProcessingTime(write_speed_3))
      //.trigger(Trigger.ProcessingTime("5 minutes"))  //SANG-IMPORTANT: 22/04/2020 --> 2x faster for collecting past data
      .start()
      .awaitTermination()



    /*
    //---21/03/2020: For testing --> omit in production run
    multiCarparkOccupancyData
       .writeStream
       .format("console")
       //.outputMode("complete") //Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
       .outputMode("append")
       .option("truncate", false)  //SANG: 21/03/2020
       .start()
       .awaitTermination()*/
  }
}
