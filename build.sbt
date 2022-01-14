name := "SP_SlotOccupancyStorage_AFog"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" //SANG: need for StructuredStreaming and SQL
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"  //SANG: just need for DStream (SparkStreming)

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" //SANG: need for StructuredStreaming and SQL