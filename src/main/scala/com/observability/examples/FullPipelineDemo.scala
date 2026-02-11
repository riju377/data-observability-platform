package com.observability.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Full Pipeline Demo -- Comprehensive showcase of ALL platform features
 *
 * Demonstrates:
 *   1. Medallion architecture (Raw -> Bronze -> Silver -> Gold)
 *   2. Diamond lineage pattern (two sources merge, then join with third)
 *   3. Multi-source ingestion (catalog table, parquet file, CSV file)
 *   4. Column lineage: DIRECT, EXPRESSION, AGGREGATE, JOIN
 *   5. Schema capture across all datasets
 *   6. Anomaly detection via 3-phase strategy (builds history, then injects spike)
 *   7. Alert triggering (requires pre-configured alert rule)
 *
 * Lineage Graph:
 *   RAW:    raw_taxi_trips (table)  raw_rideshare_trips (parquet)  raw_weather_data (CSV)
 *             |                       |                               |
 *   BRONZE: bronze_taxi_trips       bronze_rideshare_trips          bronze_weather
 *             |                       |                               |
 *   SILVER: silver_all_trips <-- UNION -----+                        |
 *             |                                                       |
 *           silver_trip_weather <------------ JOIN -------------------+
 *             |
 *   GOLD:   gold_borough_metrics  (GROUP BY borough)
 *           gold_hourly_demand    (GROUP BY hour, source)
 *
 * Anomaly Detection Strategy:
 *   The backend anomaly service requires >= 5 data points and uses 3-sigma threshold.
 *   It includes the current data point in its statistical calculation, so we need
 *   enough normal history to dilute the outlier's impact.
 *
 *   Phase 1: Full pipeline run with ~100 taxi rows (establishes lineage + 1st data point)
 *   Phase 2: 14 quick writes to bronze_taxi_trips (~100 rows each, builds history)
 *   Phase 3: Full pipeline with 1000 taxi rows (10x spike -> triggers RowCountSpike)
 *
 *   Math: With 15 normal points at ~100 and 1 anomalous at 1000:
 *     mean=156.25, stddev~225, upper_bound=831.25 -> 1000 > 831.25 = DETECTED
 *
 * PREREQUISITE -- Create an alert rule BEFORE running:
 *   curl -X POST http://localhost:8000/alert-rules \
 *     -H "Authorization: Bearer YOUR_API_KEY" \
 *     -H "Content-Type: application/json" \
 *     -d '{
 *       "name": "Demo Spike Alert",
 *       "dataset_name": "bronze_taxi_trips",
 *       "severity": "WARNING",
 *       "channel_type": "EMAIL",
 *       "channel_config": {"email_to": ["demo@example.com"]},
 *       "deduplication_minutes": 5
 *     }'
 *
 * AFTER running, verify:
 *   curl "http://localhost:8000/anomalies?dataset_name=bronze_taxi_trips&hours=1"
 *   curl "http://localhost:8000/alert-history?hours=1"
 *   curl "http://localhost:8000/datasets/bronze_taxi_trips/metrics?hours=1"
 *   curl "http://localhost:8000/datasets/silver_trip_weather/lineage"
 *   curl "http://localhost:8000/datasets/gold_borough_metrics/column-lineage"
 *
 * Usage:
 *   ./run-demo.sh
 *
 * Or manually:
 *   spark-submit \
 *     --class com.observability.examples.FullPipelineDemo \
 *     --master "local[*]" \
 *     --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
 *     --conf spark.observability.api.url=http://localhost:8000 \
 *     --conf spark.observability.api.key=YOUR_API_KEY \
 *     target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
 */
object FullPipelineDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("  FULL PIPELINE DEMO: Diamond Lineage + Anomaly Detection")
    println("=" * 80)

    val spark = SparkSession.builder()
      .appName("Full-Pipeline-Demo")
      .getOrCreate()

    val conf = spark.sparkContext.getConf
    println(s"  extraListeners = ${conf.get("spark.extraListeners", "NOT SET")}")
    println(s"  api.url        = ${conf.get("spark.observability.api.url", "NOT SET")}")
    println(s"  api.key        = ${conf.get("spark.observability.api.key", "NOT SET").take(10)}...")
    println("=" * 80)

    try {
      // Clean up from previous runs to avoid "location already exists" errors
      Seq("raw_taxi_trips", "bronze_taxi_trips", "bronze_rideshare_trips",
          "bronze_weather", "silver_all_trips", "silver_trip_weather",
          "gold_borough_metrics", "gold_hourly_demand").foreach { t =>
        spark.sql(s"DROP TABLE IF EXISTS $t")
      }

      // ============================================================
      // PHASE 1: Full pipeline run (establishes lineage + 1st data point)
      // ============================================================
      println("\n" + "=" * 70)
      println("  PHASE 1: Full pipeline run (establishes lineage graph)")
      println("  taxi=100, rideshare=50, weather=24 hours")
      println("=" * 70)
      runPipeline(spark, taxiCount = 100, rideCount = 50, weatherHours = 24)
      Thread.sleep(2000)

      // ============================================================
      // PHASE 2: Build metric history for anomaly detection
      // ============================================================
      println("\n" + "=" * 70)
      println("  PHASE 2: Building metric history for bronze_taxi_trips")
      println("  14 quick writes of ~100 rows each (need >= 5 data points for detection)")
      println("=" * 70)
      for (i <- 2 to 15) {
        val count = 98 + (i % 5) // 98-102, tight range
        generateTaxiData(spark, count)
          .filter(col("fare_amount") > 0 && col("distance_miles") > 0)
          .withColumn("source", lit("taxi"))
          .write.mode("overwrite").saveAsTable("bronze_taxi_trips")
        println(s"  History write $i/15: bronze_taxi_trips ($count rows)")
        Thread.sleep(1500)
      }
      println("  15 data points established. Anomaly detection is now active.")
      Thread.sleep(2000)

      // ============================================================
      // PHASE 3: Full pipeline with anomalous spike
      // ============================================================
      println("\n" + "=" * 70)
      println("  PHASE 3: Full pipeline with ANOMALOUS spike")
      println("  taxi=1000 (10x normal!) -- should trigger RowCountSpike WARNING")
      println("=" * 70)
      runPipeline(spark, taxiCount = 1000, rideCount = 50, weatherHours = 24)
      Thread.sleep(3000)

      // ============================================================
      // SUMMARY
      // ============================================================
      printSummary(spark)

    } catch {
      case e: Exception =>
        println(s"\nPIPELINE FAILED: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      Thread.sleep(3000)
      spark.stop()
    }
  }

  // ================================================================
  // Core Pipeline: Raw -> Bronze -> Silver -> Gold
  // ================================================================

  private def runPipeline(
    spark: SparkSession,
    taxiCount: Int,
    rideCount: Int,
    weatherHours: Int
  ): Unit = {
    val warehouseDir = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    val dataDir = s"$warehouseDir/../demo-files"

    // ---------- RAW LAYER ----------

    val rawTaxi = generateTaxiData(spark, taxiCount)
    rawTaxi.write.mode("overwrite").saveAsTable("raw_taxi_trips")
    println(s"  [RAW] raw_taxi_trips: $taxiCount rows (catalog table)")

    val rawRideshare = generateRideshareData(spark, rideCount)
    rawRideshare.write.mode("overwrite").parquet(s"$dataDir/raw_rideshare_trips")
    println(s"  [RAW] raw_rideshare_trips: $rideCount rows (parquet file)")

    val rawWeather = generateWeatherData(spark, weatherHours)
    rawWeather.write.mode("overwrite")
      .option("header", "true")
      .csv(s"$dataDir/raw_weather_data")
    println(s"  [RAW] raw_weather_data: $weatherHours rows (CSV file)")

    Thread.sleep(500)

    // ---------- BRONZE LAYER (cleaned) ----------

    // Column lineage: all DIRECT + source is EXPRESSION (lit("taxi"))
    spark.table("raw_taxi_trips")
      .filter(col("fare_amount") > 0 && col("distance_miles") > 0)
      .withColumn("source", lit("taxi"))
      .write.mode("overwrite").saveAsTable("bronze_taxi_trips")
    println(s"  [BRONZE] bronze_taxi_trips")

    // File-based lineage: parquet path -> bronze table
    spark.read.parquet(s"$dataDir/raw_rideshare_trips")
      .filter(col("fare_amount") > 0 && col("distance_miles") > 0)
      .withColumn("source", lit("rideshare"))
      .write.mode("overwrite").saveAsTable("bronze_rideshare_trips")
    println(s"  [BRONZE] bronze_rideshare_trips")

    // File-based lineage: CSV path -> bronze table
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/raw_weather_data")
      .filter(col("temperature_f").isNotNull)
      .write.mode("overwrite").saveAsTable("bronze_weather")
    println(s"  [BRONZE] bronze_weather")

    Thread.sleep(500)

    // ---------- SILVER LAYER (enriched + joined) ----------

    // UNION: bronze_taxi + bronze_rideshare -> silver_all_trips
    // Column lineage: all DIRECT from both sources
    val taxiForUnion = spark.table("bronze_taxi_trips")
      .select("trip_id", "pickup_datetime", "dropoff_datetime",
              "distance_miles", "fare_amount", "passenger_count", "borough", "source")
    val rideForUnion = spark.table("bronze_rideshare_trips")
      .select("trip_id", "pickup_datetime", "dropoff_datetime",
              "distance_miles", "fare_amount", "passenger_count", "borough", "source")

    // Column lineage: EXPRESSION (trip_duration_min, fare_per_mile, hour_of_day)
    taxiForUnion.union(rideForUnion)
      .withColumn("trip_duration_min",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)
      .withColumn("fare_per_mile",
        col("fare_amount") / col("distance_miles"))
      .withColumn("hour_of_day", hour(col("pickup_datetime")))
      .write.mode("overwrite").saveAsTable("silver_all_trips")
    println(s"  [SILVER] silver_all_trips (UNION of taxi + rideshare)")

    // JOIN: silver_all_trips + bronze_weather -> silver_trip_weather (DIAMOND pattern)
    // Column lineage: JOIN columns from both sides
    spark.table("silver_all_trips")
      .join(
        spark.table("bronze_weather").withColumnRenamed("hour_of_day", "weather_hour"),
        col("hour_of_day") === col("weather_hour"),
        "left"
      )
      .drop("weather_hour")
      .write.mode("overwrite").saveAsTable("silver_trip_weather")
    println(s"  [SILVER] silver_trip_weather (JOIN with weather) [DIAMOND]")

    Thread.sleep(500)

    // ---------- GOLD LAYER (aggregated) ----------

    // Column lineage: AGGREGATE (count, sum, avg), DIRECT (borough)
    spark.table("silver_trip_weather")
      .groupBy("borough")
      .agg(
        count("*").alias("trip_count"),
        sum("fare_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare"),
        avg("distance_miles").alias("avg_distance"),
        avg("trip_duration_min").alias("avg_duration_min"),
        sum("passenger_count").alias("total_passengers")
      )
      .write.mode("overwrite").saveAsTable("gold_borough_metrics")
    println(s"  [GOLD] gold_borough_metrics (GROUP BY borough)")

    // Column lineage: AGGREGATE (count, avg), DIRECT (hour_of_day, source)
    spark.table("silver_trip_weather")
      .groupBy("hour_of_day", "source")
      .agg(
        count("*").alias("trip_count"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_duration_min").alias("avg_duration")
      )
      .write.mode("overwrite").saveAsTable("gold_hourly_demand")
    println(s"  [GOLD] gold_hourly_demand (GROUP BY hour, source)")

    Thread.sleep(500)
  }

  // ================================================================
  // Data Generators
  // ================================================================

  private def generateTaxiData(spark: SparkSession, count: Int): DataFrame = {
    import spark.implicits._
    val boroughs = Seq("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island")

    spark.range(count).toDF("idx")
      .withColumn("trip_id", concat(lit("taxi_"), col("idx")))
      .withColumn("borough", element_at(
        array(boroughs.map(lit): _*),
        (col("idx") % boroughs.length + 1).cast("int")
      ))
      .withColumn("hour", (col("idx") % 24).cast("int"))
      .withColumn("pickup_datetime", concat(
        lit("2024-01-15 "),
        lpad(col("hour").cast("string"), 2, "0"), lit(":"),
        lpad((col("idx") % 60).cast("string"), 2, "0"), lit(":00")
      ))
      .withColumn("dropoff_datetime", concat(
        lit("2024-01-15 "),
        lpad(((col("hour") + 1) % 24).cast("string"), 2, "0"), lit(":"),
        lpad(((col("idx") + 15) % 60).cast("string"), 2, "0"), lit(":00")
      ))
      .withColumn("distance_miles", (col("idx") % 15 + 1).cast("double") + 0.5)
      .withColumn("fare_amount", col("distance_miles") * 2.5 + 2.50)
      .withColumn("passenger_count", (col("idx") % 4 + 1).cast("int"))
      .select("trip_id", "pickup_datetime", "dropoff_datetime",
              "distance_miles", "fare_amount", "passenger_count", "borough")
  }

  private def generateRideshareData(spark: SparkSession, count: Int): DataFrame = {
    import spark.implicits._
    val boroughs = Seq("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island")

    spark.range(count).toDF("idx")
      .withColumn("trip_id", concat(lit("ride_"), col("idx")))
      .withColumn("borough", element_at(
        array(boroughs.map(lit): _*),
        (col("idx") % boroughs.length + 1).cast("int")
      ))
      .withColumn("hour", (col("idx") % 24).cast("int"))
      .withColumn("pickup_datetime", concat(
        lit("2024-01-15 "),
        lpad(col("hour").cast("string"), 2, "0"), lit(":"),
        lpad(((col("idx") + 30) % 60).cast("string"), 2, "0"), lit(":00")
      ))
      .withColumn("dropoff_datetime", concat(
        lit("2024-01-15 "),
        lpad(((col("hour") + 1) % 24).cast("string"), 2, "0"), lit(":"),
        lpad(((col("idx") + 40) % 60).cast("string"), 2, "0"), lit(":00")
      ))
      .withColumn("distance_miles", (col("idx") % 12 + 1).cast("double") + 0.3)
      .withColumn("fare_amount", col("distance_miles") * 2.0 + 3.00)
      .withColumn("passenger_count", (col("idx") % 3 + 1).cast("int"))
      .select("trip_id", "pickup_datetime", "dropoff_datetime",
              "distance_miles", "fare_amount", "passenger_count", "borough")
  }

  private def generateWeatherData(spark: SparkSession, hours: Int): DataFrame = {
    import spark.implicits._
    val conditions = Seq("Clear", "Cloudy", "Rain", "Snow", "Fog")

    spark.range(hours).toDF("idx")
      .withColumn("hour_of_day", col("idx").cast("int"))
      .withColumn("temperature_f", (col("idx") % 30 + 25).cast("double"))
      .withColumn("humidity_pct", (col("idx") % 50 + 30).cast("double"))
      .withColumn("wind_speed_mph", (col("idx") % 20 + 2).cast("double"))
      .withColumn("precipitation_in",
        when(col("idx") % 5 === 0, 0.3).otherwise(0.0))
      .withColumn("condition", element_at(
        array(conditions.map(lit): _*),
        (col("idx") % conditions.length + 1).cast("int")
      ))
      .select("hour_of_day", "temperature_f", "humidity_pct",
              "wind_speed_mph", "precipitation_in", "condition")
  }

  // ================================================================
  // Summary
  // ================================================================

  private def printSummary(spark: SparkSession): Unit = {
    println("\n" + "=" * 80)
    println("  PIPELINE COMPLETE")
    println("=" * 80)

    val datasets = Seq(
      "raw_taxi_trips", "bronze_taxi_trips", "bronze_rideshare_trips", "bronze_weather",
      "silver_all_trips", "silver_trip_weather", "gold_borough_metrics", "gold_hourly_demand"
    )

    println("\n  Final row counts:")
    datasets.foreach { name =>
      try {
        val count = spark.table(name).count()
        println(f"    $name%-30s $count%6d rows")
      } catch { case _: Exception => println(s"    $name -- NOT FOUND") }
    }

    println("\n  Lineage graph:")
    println("    raw_taxi_trips       -> bronze_taxi_trips       -+")
    println("    raw_rideshare (file) -> bronze_rideshare_trips  -+-> silver_all_trips -+")
    println("    raw_weather (file)   -> bronze_weather ----------+                     |")
    println("                                                     |   (JOIN)            |")
    println("                                                     +-> silver_trip_weather")
    println("                                                           |")
    println("                                                           +-> gold_borough_metrics")
    println("                                                           +-> gold_hourly_demand")

    println("\n" + "-" * 80)
    println("  ANOMALY DETECTION VERIFICATION")
    println("-" * 80)
    println("  After 15 normal writes (~100 rows) + 1 anomalous write (1000 rows),")
    println("  the API should have detected a RowCountSpike for bronze_taxi_trips.")
    println("")
    println("  Check anomalies:")
    println("    curl \"http://localhost:8000/anomalies?dataset_name=bronze_taxi_trips&hours=1\"")
    println("")
    println("  Check metrics history:")
    println("    curl \"http://localhost:8000/datasets/bronze_taxi_trips/metrics?hours=1\"")
    println("")
    println("  Check alert history:")
    println("    curl \"http://localhost:8000/alert-history?hours=1\"")
    println("")
    println("  View lineage:")
    println("    curl \"http://localhost:8000/datasets/silver_trip_weather/lineage\"")
    println("    curl \"http://localhost:8000/datasets/gold_borough_metrics/column-lineage\"")
    println("")
    println("  View in UI:")
    println("    http://localhost:3000")
    println("=" * 80)
  }
}
