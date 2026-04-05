import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count as f_count, sum as f_sum,
    avg as f_avg, min as f_min, max as f_max, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "traffic_iot")

PG_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/traffic")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

schema = StructType([
    StructField("event_time", StringType(), False),
    StructField("sensor_id", StringType(), False),
    StructField("location", StringType(), False),
    StructField("vehicle_count", IntegerType(), False),
    StructField("avg_speed_kmh", DoubleType(), False),
    StructField("occupancy_pct", DoubleType(), False),
    StructField("incident", BooleanType(), False),
])

def write_batch(df, table: str):
    (df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

def main():
    spark = (SparkSession.builder
             .appName("iot-traffic-analytics")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("v"))
              .select("v.*"))

    # event time + basic cleaning
    events = (parsed
              .withColumn("event_ts", to_timestamp(col("event_time")))
              .drop("event_time")
              # Filter invalid / out-of-range readings
              .filter(col("event_ts").isNotNull())
              .filter((col("avg_speed_kmh") >= 0) & (col("avg_speed_kmh") <= 140))
              .filter((col("occupancy_pct") >= 0) & (col("occupancy_pct") <= 100))
              .filter((col("vehicle_count") >= 0) & (col("vehicle_count") <= 200))
              )

    # Watermark for event-time windows (handles late data)
    events_wm = events.withWatermark("event_ts", "2 minutes")

    def agg_for(duration: str, table: str):
        agged = (events_wm
                 .groupBy(window(col("event_ts"), duration), col("sensor_id"), col("location"))
                 .agg(
                     f_count("*").alias("events"),
                     f_sum("vehicle_count").alias("vehicles_sum"),
                     f_avg("avg_speed_kmh").alias("speed_avg"),
                     f_min("avg_speed_kmh").alias("speed_min"),
                     f_max("avg_speed_kmh").alias("speed_max"),
                     f_avg("occupancy_pct").alias("occupancy_avg"),
                 )
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     "sensor_id",
                     "location",
                     "events",
                     "vehicles_sum",
                     expr("CAST(speed_avg AS DOUBLE)").alias("speed_avg"),
                     expr("CAST(speed_min AS DOUBLE)").alias("speed_min"),
                     expr("CAST(speed_max AS DOUBLE)").alias("speed_max"),
                     expr("CAST(occupancy_avg AS DOUBLE)").alias("occupancy_avg"),
                 ))

        query = (agged.writeStream
                 .outputMode("update")
                 .foreachBatch(lambda df, epoch_id: write_batch(df, table))
                 .option("checkpointLocation", f"/tmp/chk_{table}")
                 .start())
        return query

    q1 = agg_for("1 minute", "traffic_agg_1m")
    q5 = agg_for("5 minutes", "traffic_agg_5m")

    # --- Simple anomaly detection ---
    # We compute per-sensor rolling stats using 5-minute windows and then flag outliers at event level
    # by joining event stream with latest stats for that sensor/window.
    stats_5m = (events_wm
                .groupBy(window(col("event_ts"), "5 minutes"), col("sensor_id"), col("location"))
                .agg(
                    f_avg("avg_speed_kmh").alias("mean_speed"),
                    expr("stddev_samp(avg_speed_kmh)").alias("std_speed")
                )
                .select(
                    col("window.start").alias("w_start"),
                    col("window.end").alias("w_end"),
                    "sensor_id",
                    "location",
                    col("mean_speed"),
                    col("std_speed")
                ))

    # Join each event to its 5-minute stats window (event_ts between w_start and w_end)
    events_with_stats = (events_wm
                         .join(
                             stats_5m,
                             on=[
                                 events_wm.sensor_id == stats_5m.sensor_id,
                                 events_wm.location == stats_5m.location,
                                 (events_wm.event_ts >= stats_5m.w_start) & (events_wm.event_ts < stats_5m.w_end)
                             ],
                             how="inner"
                         )
                         .drop(stats_5m.sensor_id)
                         .drop(stats_5m.location)
                         )

    # robust-ish z score: if std is tiny/null, avoid division
    anomalies = (events_with_stats
                 .withColumn("speed_z",
                             expr("CASE WHEN std_speed IS NULL OR std_speed < 0.0001 THEN 0.0 "
                                  "ELSE (avg_speed_kmh - mean_speed) / std_speed END"))
                 .withColumn("processing_time", current_timestamp())
                 .filter(expr("ABS(speed_z) >= 3.0"))
                 .select(
                     col("event_ts").alias("event_time"),
                     col("processing_time"),
                     "sensor_id",
                     "location",
                     col("avg_speed_kmh"),
                     col("vehicle_count"),
                     col("occupancy_pct"),
                     col("speed_z"),
                     expr("CASE WHEN speed_z <= -3 THEN 'SPEED_DROP' ELSE 'SPEED_SPIKE' END").alias("reason")
                 ))

    qA = (anomalies.writeStream
          .outputMode("append")
          .foreachBatch(lambda df, epoch_id: write_batch(df, "traffic_anomalies"))
          .option("checkpointLocation", "/tmp/chk_traffic_anomalies")
          .start())

    # Wait for all queries
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()