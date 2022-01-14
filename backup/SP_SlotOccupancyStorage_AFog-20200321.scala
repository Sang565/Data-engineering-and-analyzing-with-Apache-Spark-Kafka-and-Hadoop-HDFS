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
 *
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

    //***************************************************************************************************
    //=== Step 1: Create a Spark session and variables: topic, kafka brokers =======
    val sparkMaster = "spark://hpc-master:7077"
    //val checkpointLocation = "file:///home/pi/spark-applications/spark-applications/checkpoint" //SANG: use local storage on Spark's nodes --> copy the same files and folder to ALL nodes
    val checkpointLocation = "/checkpoint" //SANG: edit to HDFS path

    val kafkaBrokers_hpc = "hpc-kafka01:9092"
    val slotOccupancyInputTopic = "sp-slot-occupancy"

    val spark = SparkSession.builder()
      .appName("SP_SlotOccupancyStorageCSV_AFog")
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
      .option("kafka.bootstrap.servers", kafkaBrokers_hpc)
      .option("subscribe", slotOccupancyInputTopic) //subscribe Kafka topic name: sp-topic
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

    //--- Step 2.3: parse data for processing in the next stage  -------
    //--- 2.3.1: define a schema for parsing
    val dataStreamSchema = new StructType()
      .add("carparkID", StringType)
      //.add("current-time", StringType)  //SANG: 21/03/2020 --> current-time is the event-time in carpark-occupancy record sent from S-Fog
      .add("processing-time", StringType)  //SANG: 21/03/2020
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
    multiCarparkOccupancyData.printSchema   //SANG: ko co dau () o sau Schema

    //==================================================================================
    //--- 4.2.1.3: output result --> Chu y .trigger(Trigger.ProcessingTime("1 minute"))
    /* //----- for CSV format
    multiCarparkOccupancyData
      .withWatermark("processing-time", "5 milliseconds") //SANG: unused
      //.selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")  //SANG-IMPORTANT: 21/03/2020
         //--- If has the above line, the record formate will be quite strange --> hardly to process
             // {"key":"1","value":"{\"processing-time\":\"2020-03-21T18:07:50.030+13:00\",\"carparkID\":\"1\",\"slotOccupancy\":151}"}
         //--- Without the line: {"processing-time":"2020-03-22T10:40:00.023+13:00","carparkID":"1","slotOccupancy":52}
      .writeStream
      .format("csv")
      //.outputMode("complete")  //SANG: Ko support cho File Sink
      //.outputMode("update")   //SANG: Ko support cho File Sink
      .outputMode("append") //SANG: Voi CSV thi ko support updata, che do File Sink chi support Append mode without doing Aggregate task
                                        //https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#output-sinks
      .option("checkpoint", "/checkpoint/")
      .option("path", "/multiCarparkOccupancyStorage_CSV/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      //.trigger(Trigger.ProcessingTime("1 minute"))
      .start()
    //.awaitTermination()*/

    /* //----- for parquet format
    multiCarparkOccupancyData
      .withWatermark("processing-time", "5 milliseconds") //SANG: unused
      //.selectExpr ("CAST(carparkID AS STRING) AS key", "to_json(struct(*)) AS value")  //SANG-IMPORTANT: 21/03/2020
         //--- If has the above line, the record formate will be quite strange --> hardly to process
             // {"key":"1","value":"{\"processing-time\":\"2020-03-21T18:07:50.030+13:00\",\"carparkID\":\"1\",\"slotOccupancy\":151}"}
         //--- Without the line: {"processing-time":"2020-03-22T10:40:00.023+13:00","carparkID":"1","slotOccupancy":52}
      .writeStream
      .format("parquet")
      //.outputMode("complete")  //SANG: Ko support cho File Sink
      //.outputMode("update")   //SANG: Ko support cho File Sink
      .outputMode("append") //SANG: Voi CSV thi ko support updata, che do File Sink chi support Append mode without doing Aggregate task
      //https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#output-sinks
      .option("checkpoint", "/checkpoint/")
      .option("path", "/multiCarparkOccupancyStorage_parquet/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      //.trigger(Trigger.ProcessingTime("1 minute"))
      .start()
    //.awaitTermination()*/

    multiCarparkOccupancyData
      .withWatermark("processing-time", "5 milliseconds") //SANG: unused
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

      .option("path", "/multiCarparkOccupancyStorage_JSON_20200321/")	//SANG: topic --> phai co dau / sau chu Storage thi no moi hieu duong dan
      //.option("path", "/multiCarparkOccupancyStorage_JSON/")   //SANG: for production

      .trigger(Trigger.ProcessingTime("2 minutes"))  //SANG-IMPORTANT: 21/03/2020, giam phan manh (write to so mamy small files)
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
