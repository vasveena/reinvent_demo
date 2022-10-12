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
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import java.util.HashMap
import org.apache.hudi._
import scala.reflect.io.Directory
import java.io.File

object MTASubwayTripUpdatesMSK extends Serializable {
  def main(args: Array[String]) { 

    //Delete checkpoint dir 

    val directory = new Directory(new File("/tmp/checkpoint"))
    directory.deleteRecursively()

    //Initialize Spark session    

    val spark = SparkSession.builder().appName("MTA Trip Streaming ETL for Amazon MSK").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val trip_update_topic = "trip_update_topic"
    val trip_status_topic = "trip_status_topic"
    val broker = "b-3.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094,b-1.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094,b-2.democluster2.exng22.c17.kafka.us-east-1.amazonaws.com:9094"

    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    @transient var producer : KafkaProducer[String, String] = null
    @transient var joined_query : StreamingQuery = null
    @transient var joined_query_s3 : StreamingQuery = null

    def start() = {
        //Start producer for kafka

        producer = new KafkaProducer[String, String](props)

        //Create a datastream from trip update topic

        val trip_update_df = spark.readStream
				  .format("kafka")
			          .option("kafka.bootstrap.servers", broker)
        			  .option("kafka.security.protocol","SSL")
        			  .option("subscribe", trip_update_topic)
        			  .option("startingOffsets", "latest").option("failOnDataLoss","false").load()

        //Create a datastream from trip status topic
        val trip_status_df = spark.readStream
        			  .format("kafka")
        			  .option("kafka.bootstrap.servers", broker)
        			  .option("kafka.security.protocol","SSL")
        			  .option("subscribe", trip_status_topic)
        			  .option("startingOffsets", "latest").option("failOnDataLoss","false").load()

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

	val trip_update_ds = trip_update_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

        val trip_update_ds_schema = trip_update_ds.select(from_json($"value", trip_update_schema).as("data")).select("data.*")
        trip_update_ds_schema.printSchema()

        val trip_status_ds = trip_status_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

        val trip_status_ds_schema = trip_status_ds.select(from_json($"value", trip_status_schema).as("data")).select("data.*")
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
 
        //Console for testing purpose only. Commenting. 
        /* val joined_query = joined_ds.writeStream.outputMode("complete").format("console")
        .option("truncate", "true").outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime("10 seconds")).start() */
    
        val joined_query_s3 = joined_ds.writeStream
		               .outputMode("append")
                               .format("parquet")
                               .queryName("MTA")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .trigger(Trigger.ProcessingTime("10 seconds"))
                               .option("path", "s3://vasveena-test-demo/reinvent/streaming_etl_msk_output/")
                               .start()
                               .awaitTermination()    
    }
   start
  }
}

// scalastyle:on println
