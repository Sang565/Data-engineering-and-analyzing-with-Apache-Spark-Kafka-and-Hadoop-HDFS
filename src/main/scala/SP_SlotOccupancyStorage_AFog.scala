/*
 * ======================================================================================
 * - Functions:
 *  This Spark-based code is run at the A-Fog tier to catch carparks’ occupancies data streamed from multiple S-Fog systems, then store these data in the A-Fog’s HDFS for long-term data storage and large-scale data analytics later. 
 *
 * - Technologies: Spark-Kafka Integration, Spark Structured Streaming, Hadoop HDFS
 * =======================================================================================
 * 29/05/2020
 *   Add 2 args for write_speed on HDFS storage 2 and topic 3 in order to get data in the corresponding time with generating_speed in SFog for simulation
 *
 * 10/06/2020
 *   Change to use with afog-master and Spark 2.4.5 (build.sbt)
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.sql.streaming.Trigger 

object SP_SlotOccupancyStorage_AFog {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: SP_SlotZoneOccupancy_SFog2 <write_speed_2>, <write_speed_3>")
      System.exit(1)
    }

    val write_speed_2 = args(0)
    val write_speed_3 = args(1)
    //***************************************************************************************************
    //=== Step 1: Create a Spark session and variables: topic, kafka brokers =======
    val sparkMaster = "spark://afog-master:7077"

    val checkpointLocation = "/checkpoint" // HDFS path

    val kafkaBrokers_afog = "afog-kafka01:9092"
    val slotOccupancyInputTopic = "sp-slot-occupancy"
    val slotOccupancyInputTopic2 = "sp-slot-occupancy-2"  
    val slotOccupancyInputTopic3 = "sp-slot-occupancy-3"  

    val spark = SparkSession.builder()
      .appName("SP_SlotOccupancyStorage_AFog")
      .master(sparkMaster)
      .config("spark.sql.streaming.checkpointLocation", checkpointLocation) 
      .getOrCreate()

    import spark.implicits._
    val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR) //only display ERROR, not display WARN

    //***************************************************************************************************
    //=== Step 2: Read streaming messages from Kafka topics, select desired data and parse on this data for processing =======
    //--- Step 2.1: Read and parse node mapping files (CSV file) --------------------

    //--- Step 2.2: Read streaming messages and extract desired elements (e.g. value) from Kafka topics -------
    //--- 2.2.1: Setup connection to Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic) //subscribe Kafka topic name: sp-topic
      .option("startingOffsets", "latest")
      .load()

    // add more streams collecting on slotOccupancyInputTopic-2 and -3
    val kafka2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic2) //subscribe Kafka topic name: sp-topic
      .option("startingOffsets", "latest")
      .load()

    val kafka3 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers_afog)
      .option("subscribe", slotOccupancyInputTopic3) //subscribe Kafka topic name: sp-topic
      .option("startingOffsets", "latest")
      .load()

    //--- 2.2.2: Selecting desired data read from Kafka topic
    val kafkaData = kafka
      //.withColumn("Key", $"key".cast(StringType))	//add "Key" column from $"key" element
      //.withColumn("Topic", $"topic".cast(StringType))
      //.withColumn("Offset", $"offset".cast(LongType))
      //.withColumn("Partition", $"partition".cast(IntegerType))
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) // this timestamp belongs to Kafka
      .withColumn("Value", $"value".cast(StringType))
      //.select("topic", "Key", "Value", "Partition", "Offset", "Timestamp")
      .select("Value", "Timestamp")
    val rawData = kafkaData.selectExpr("CAST(Value as STRING)")

    // add more streams collecting on slotOccupancyInputTopic-2 and -3
    val kafkaData2 = kafka2
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) 
      .withColumn("Value", $"value".cast(StringType))
      .select("Value", "Timestamp")
    val rawData2 = kafkaData2.selectExpr("CAST(Value as STRING)")

    val kafkaData3 = kafka3
      .withColumn("Timestamp", $"timestamp".cast(TimestampType)) 
      .withColumn("Value", $"value".cast(StringType))
      .select("Value", "Timestamp")
    val rawData3 = kafkaData3.selectExpr("CAST(Value as STRING)")

    //--- Step 2.3: parse data for processing in the next stage  -------
    //--- 2.3.1: define a schema for parsing
    val dataStreamSchema = new StructType()
      .add("carparkID", StringType)
      .add("processing-time", StringType)  
      .add("slotOccupancy", StringType)

    val dataStreamSchema2 = new StructType()
      .add("carparkID", StringType)
      .add("event-time", StringType)  
      .add("slotOccupancy", StringType)

    //--- 2.3.2: parse data for doing processing in the next stage
    val multiCarparkOccupancyData = rawData
      .select(from_json(col("Value"), dataStreamSchema).alias("multiCarparkOccupancyData")) // Value from Kafka topic
      .select("multiCarparkOccupancyData.*") 
      .select($"processing-time", $"carparkID", $"slotOccupancy") 
      .withColumn("processing-time", $"processing-time".cast(TimestampType))	
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))
      .select("processing-time", "carparkID", "slotOccupancy") 

    println("\n=== multiCarparkOccupancyData schema ====")
    multiCarparkOccupancyData.printSchema()   

    // add more streams collecting on slotOccupancyInputTopic-2 and -3
    val multiCarparkOccupancyData2 = rawData2
      .select(from_json(col("Value"), dataStreamSchema2).alias("multiCarparkOccupancyData2")) 
      .select("multiCarparkOccupancyData2.*") 
      .select($"event-time", $"carparkID", $"slotOccupancy") 
      .withColumn("processing-time", $"event-time".cast(TimestampType))	
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))
      .select("processing-time", "carparkID", "slotOccupancy") 

    println("\n=== multiCarparkOccupancyData2 schema ====")
    //multiCarparkOccupancyData2.printSchema()   

    val multiCarparkOccupancyData3 = rawData3
      .select(from_json(col("Value"), dataStreamSchema2).alias("multiCarparkOccupancyData3")) 
      .select("multiCarparkOccupancyData3.*") 
      .select($"event-time", $"carparkID", $"slotOccupancy") 
      .withColumn("processing-time", $"event-time".cast(TimestampType))	
      .withColumn("carparkID", $"carparkID".cast(StringType))
      .withColumn("slotOccupancy", $"slotOccupancy".cast(IntegerType))
      .select("processing-time", "carparkID", "slotOccupancy") 

    println("\n=== multiCarparkOccupancyData3 schema ====")
    //multiCarparkOccupancyData3.printSchema()   

    //==================================================================================
    //--- 4.2.1.3: output result 
    multiCarparkOccupancyData
      .withWatermark("processing-time", "10 milliseconds") 
      .writeStream
      .format("json")
      .outputMode("append") //File Sink only support Append mode without doing Aggregate task --> https://spark.apache.org/docs/2.3.3/structured-streaming-programming-guide.html#output-sinks
      .option("truncate", false) 
      .option("checkpoint", "/checkpoint/")
      .option("path", "/multiCarparkOccupancyStorage_JSON-1/")	
      .trigger(Trigger.ProcessingTime("10 minutes")) 
      .start()
    //.awaitTermination()

    // -- add more streams collecting on slotOccupancyInputTopic-2 and -3
    //val write_speed_2 = args(0)
    multiCarparkOccupancyData2
      .withWatermark("processing-time", "20 milliseconds") 
      .writeStream
      .format("json")
      .outputMode("append")
      .option("truncate", false) 
      .option("checkpoint", "/checkpoint/")
      .option("path", "/multiCarparkOccupancyStorage_JSON-2/")   
      .trigger(Trigger.ProcessingTime(write_speed_2))
      //.trigger(Trigger.ProcessingTime("5 minutes"))  //IMPORTANT:  2x faster for collecting past data
      .start()
    //.awaitTermination()

    //val write_speed_3 = args(3)
    multiCarparkOccupancyData3
      .withWatermark("processing-time", "20 milliseconds") 
      .writeStream
      .format("json")
      .outputMode("append")
      .option("truncate", false) 
      .option("checkpoint", "/checkpoint/")
      .option("path", "/multiCarparkOccupancyStorage_JSON-3/")   
      .trigger(Trigger.ProcessingTime(write_speed_3))
      //.trigger(Trigger.ProcessingTime("5 minutes"))  //IMPORTANT: 2x faster for collecting past data
      .start()
      .awaitTermination()
  }
}
