/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package reinvent.streaming

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import java.util.HashMap
import org.apache.hudi._
import scala.reflect.io.Directory
import java.io.File

object MTASubwayTripUpdatesKinesisWithHudi extends Serializable {
  def main(args: Array[String]) { 

    //Delete checkpoint dir 
    
    val directory = new Directory(new File("/tmp/checkpoint"))
    directory.deleteRecursively()

    //Initialize Spark session    
    
    val spark = SparkSession.builder().appName("MTA Trip Streaming ETL for Amazon Kinesis stream").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // General Constants
    
    val HUDI_FORMAT = "org.apache.hudi"
    val TABLE_NAME = "hoodie.table.name"
    val RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field"
    val PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field"
    val OPERATION_OPT_KEY = "hoodie.datasource.write.operation"
    val UPSERT_OPERATION_OPT_VAL = "upsert"
    val UPSERT_PARALLELISM = "hoodie.upsert.shuffle.parallelism"
    val S3_CONSISTENCY_CHECK = "hoodie.consistency.check.enabled"
    val HUDI_CLEANER_POLICY = "hoodie.cleaner.policy"
    val KEEP_LATEST_COMMITS = "KEEP_LATEST_COMMITS"
    val TABLE_TYPE_OPT_KEY="hoodie.datasource.write.table.type"

    // Hive Constants
    
    val HIVE_SYNC_ENABLED_OPT_KEY="hoodie.datasource.hive_sync.enable"
    val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY="hoodie.datasource.hive_sync.partition_extractor_class"

    // Partition Constants
    
    val NONPARTITION_EXTRACTOR_CLASS_OPT_VAL="org.apache.hudi.hive.NonPartitionedExtractor"
    val KEYGENERATOR_CLASS_OPT_KEY="hoodie.datasource.write.keygenerator.class"
    val NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL="org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    @transient var joined_query : StreamingQuery = null
    @transient var joined_query_s3 : StreamingQuery = null

    def start() = {

	//Read from trip update Kinesis data stream
        
        val trip_update_df = spark
                                .readStream
                                .format("kinesis")
                                .option("streamName", "trip-update-stream")
                                .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
                                .option("startingposition", "LATEST")
                                .load

        //Read from trip status Kinesis data stream
        
        val trip_status_df = spark
                                .readStream
                                .format("kinesis")
                                .option("streamName", "trip-status-stream")
                                .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com")
                                .option("startingposition", "LATEST")
                                .load

        // define schema of data

	val trip_update_schema = new StructType()
                                        .add("trip", new StructType().add("tripId","string").add("startTime","string").add("startDate","string").add("routeId","string"))
                                        .add("stopTimeUpdate",ArrayType(new StructType().add("arrival",new StructType().add("time","string")).add("stopId","string").add("departure",new StructType().add("time","string"))))

        val trip_status_schema = new StructType()
                                        .add("trip", new StructType().add("tripId","string").add("startTime","string").add("startDate","string").add("routeId","string"))
                                        .add("currentStopSequence","integer")
                                        .add("currentStatus", "string")
                                        .add("timestamp", "string")
                                        .add("stopId","string")

        // covert datastream into a datasets and apply schema
        
	val trip_update_ds = trip_update_df.selectExpr("CAST(partitionKey AS STRING)", "CAST(data AS STRING)").as[(String, String)]

        val trip_update_ds_schema = trip_update_ds.select(from_json($"data", trip_update_schema).as("data")).select("data.*")
        trip_update_ds_schema.printSchema()

        val trip_status_ds = trip_status_df.selectExpr("CAST(partitionKey AS STRING)", "CAST(data AS STRING)").as[(String, String)]

        val trip_status_ds_schema = trip_status_ds.select(from_json($"data", trip_status_schema).as("data")).select("data.*")
        trip_status_ds_schema.printSchema()

	val trip_status_ds_unnest = trip_status_ds_schema.select("trip.*","currentStopSequence","currentStatus","stopId")
                              .withColumn("currentStatus", when($"currentStatus" === "STOPPED_AT", $"currentStatus").otherwise(lit("ON_THE_ROAD")))
                              .withColumnRenamed("stopId","currentStopId")

        val trip_update_ds_unnest = trip_update_ds_schema.select($"trip.*", $"stopTimeUpdate.arrival.time".as("arrivalTime"),
                $"stopTimeUpdate.departure.time".as("departureTime"), $"stopTimeUpdate.stopId")

        val trip_update_ds_unnest2 = trip_update_ds_unnest.withColumn("numOfFutureStops", size($"arrivalTime"))
                                                          .withColumnRenamed("stopId","futureStopIds")
                                                          .withColumn("startDateStr",date_format(to_date($"startDate","yyyyMMdd"), "yyyy-MM-dd"))

        val joined_ds = trip_update_ds_unnest2
                                .join(trip_status_ds_unnest, Seq("tripId","routeId","startTime","startDate"))
				.withColumn("numOfFutureStops", when($"numOfFutureStops" >= 0, $"numOfFutureStops").otherwise(lit(0)))
                                .withColumn("startTime", to_utc_timestamp(date_format(concat($"startDateStr", lit(" "), $"startTime"), "yyyy-MM-dd HH:mm:ss"),"America/Los_Angeles"))
                                .withColumn("nextStopArrivalTime", to_utc_timestamp(from_unixtime($"arrivalTime".getItem(0)),"America/Los_Angeles"))
                                .withColumn("nextStopId", $"futureStopIds".getItem(0))
                                .withColumn("lastStopArrivalTime", to_utc_timestamp(from_unixtime($"arrivalTime".apply(size($"arrivalTime").minus(1))),"America/Los_Angeles"))
                                .withColumn("lastStopId", $"futureStopIds".apply(size($"futureStopIds").minus(1)))
                                .withColumn("currentTime", to_utc_timestamp(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"),"America/Los_Angeles"))
                                .drop("startDate","startDateStr","arrivalTime","departureTime","futureStopIds")
                                

        joined_ds.printSchema()

	def myFunc( batchDF:DataFrame, batchID:Long ) : Unit = {
            batchDF.persist()
            batchDF.write.format("org.apache.hudi")
                .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
                .option(PRECOMBINE_FIELD_OPT_KEY, "currentTime")
                .option(RECORDKEY_FIELD_OPT_KEY, "tripId")
                .option(TABLE_NAME, "hudi_trips_streaming_table")
                .option(UPSERT_PARALLELISM, 200)
                .option(HUDI_CLEANER_POLICY, KEEP_LATEST_COMMITS)
                .option(S3_CONSISTENCY_CHECK, "true")
                .option(HIVE_SYNC_ENABLED_OPT_KEY,"true")
                .option(OPERATION_OPT_KEY, UPSERT_OPERATION_OPT_VAL)
                .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY,NONPARTITION_EXTRACTOR_CLASS_OPT_VAL)
                .option(KEYGENERATOR_CLASS_OPT_KEY,NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL)
                .mode(SaveMode.Append)
                .save("s3://vasveena-test-demo/reinvent/streaming_etl_kinesis_with_hudi_output/")
            
            batchDF.unpersist()
        }

        //Console - only for testing purpose. Commenting for now. 

        /* val joined_query = joined_ds.writeStream.outputMode("complete").format("console")
        .option("truncate", "true").outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime("10 seconds")).start() */
        
        val joined_query_s3 = joined_ds.writeStream
                                       .queryName("MTA")
        			       .trigger(Trigger.ProcessingTime("10 seconds"))
        			       .foreachBatch(myFunc _)
        			       .option("checkpointLocation", "/tmp/checkpoint")
        			       .start()
        			       .awaitTermination()
    }
   start
  }
}

// scalastyle:on println
