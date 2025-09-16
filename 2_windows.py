from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import SparkSession
from configs import kafka_config, prefix

import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 "
    "pyspark-shell"
)

topic_name = f'{prefix}_building_sensors_4'

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.driver.maxResultSize", "1g") \
         .getOrCreate())

alerts_conditions_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

json_schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("value", DoubleType())
])


# Маніпуляції з даними
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .select(
        col("value_json.sensor_type").alias("sensor_type"),
        col("value_json.value").alias("value"),
        to_timestamp(col("value_json.timestamp")).alias("timestamp")
    )

agg_df = (clean_df
          .withWatermark("timestamp", "10 seconds")
          .groupBy(
              window(col("timestamp"), "1 minute", "30 seconds")
          )
          .agg(
              avg(when(col("sensor_type") == "temperature", col("value"))).alias("temperature_avg"),
              avg(when(col("sensor_type") == "humidity", col("value"))).alias("humidity_avg")
          )
)

joined = agg_df.crossJoin(alerts_conditions_df)

alert_condition = (
    (col("humidity_avg") >= col("humidity_min")) & (col("humidity_avg") <= col("humidity_max")) |
    (col("temperature_avg") >= col("temperature_min")) & (col("temperature_avg") <= col("temperature_max"))
)

alerts_filtered = joined.filter(alert_condition).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("temperature_avg"),
    col("humidity_avg"),
    col("code"),
    col("message")
)

prepare_to_kafka_df = alerts_filtered.selectExpr(
    "CAST(code AS STRING) AS key",
    """to_json(named_struct(
            'window_start', window_start,
            'window_end', window_end,
            'temperature_avg', temperature_avg,
            'humidity_avg', humidity_avg,
            'code', code,
            'message', message
        )) AS value"""
)

kafka_query = prepare_to_kafka_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", f"{prefix}_general_alerts") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-3") \
    .start()

query = (agg_df.writeStream
         .outputMode("update") 
         .format("console")
         .option("truncate", False)
         .option("checkpointLocation", "/tmp/checkpoints-2")
         .start())

spark.streams.awaitAnyTermination()