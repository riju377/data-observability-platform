package com.observability.examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * Schema Evolution Demo -- Tracks 5 schema versions via the listener
 *
 * The ObservabilityListener automatically captures schema on every write.
 * This demo writes to the same table 5 times with different schemas:
 *
 *   V1: id(INT), name(STRING), age(INT)                     -- Initial
 *   V2: id, name, age, email(STRING), city(STRING)           -- ADD columns
 *   V3: id, name, age(STRING), email, city                   -- TYPE change (INT->STRING)
 *   V4: id, name, age, city                                  -- REMOVE column (email)
 *   V5: id(NOT NULL), name(NOT NULL), age, city              -- NULLABILITY change
 *
 * This file contains ZERO com.observability imports.
 * The listener is registered transparently via spark.extraListeners.
 *
 * Usage:
 *   ./run-schema-demo.sh
 *
 * Or manually:
 *   spark-submit \
 *     --class com.observability.examples.SchemaEvolutionDemo \
 *     --master "local[*]" \
 *     --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
 *     --conf spark.observability.api.url=http://localhost:8000 \
 *     --conf spark.observability.api.key=YOUR_API_KEY \
 *     target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
 *
 * Verify after running:
 *   curl http://localhost:8000/datasets/demo_customers/schema/history
 *   curl http://localhost:8000/datasets/demo_customers/schema
 */
object SchemaEvolutionDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("  SCHEMA EVOLUTION DEMO")
    println("=" * 60)

    val spark = SparkSession.builder()
      .appName("Schema-Evolution-Demo")
      .getOrCreate()

    val conf = spark.sparkContext.getConf
    println(s"  extraListeners = ${conf.get("spark.extraListeners", "NOT SET")}")
    println(s"  api.url        = ${conf.get("spark.observability.api.url", "NOT SET")}")
    println("=" * 60)

    try {
      import spark.implicits._

      // Clean up from previous runs to avoid "location already exists" errors
      spark.sql("DROP TABLE IF EXISTS demo_customers")

      // V1: Initial schema
      println("\n[V1] demo_customers: id(INT), name(STRING), age(INT)")
      Seq((1, "Alice", 30), (2, "Bob", 25), (3, "Carol", 35))
        .toDF("id", "name", "age")
        .write.mode("overwrite").saveAsTable("demo_customers")
      Thread.sleep(2000)

      // V2: Add columns (non-breaking)
      println("[V2] Adding email and city columns")
      Seq(
        (1, "Alice", 30, "alice@test.com", "New York"),
        (2, "Bob",   25, "bob@test.com",   "Boston"),
        (3, "Carol", 35, "carol@test.com", "Chicago"),
        (4, "David", 28, "david@test.com", "Seattle")
      ).toDF("id", "name", "age", "email", "city")
        .write.mode("overwrite").saveAsTable("demo_customers")
      Thread.sleep(2000)

      // V3: Type change - age INT -> STRING (breaking)
      println("[V3] Changing age type from INT to STRING (BREAKING)")
      val v3Schema = StructType(Array(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("age", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("city", StringType, nullable = true)
      ))
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, "Alice", "30", "alice@test.com", "New York"),
          Row(2, "Bob",   "25", "bob@test.com",   "Boston")
        )),
        v3Schema
      ).write.mode("overwrite").saveAsTable("demo_customers")
      Thread.sleep(2000)

      // V4: Remove column - email (breaking)
      println("[V4] Removing email column (BREAKING)")
      Seq(
        (1, "Alice", "30", "New York"),
        (2, "Bob",   "25", "Boston"),
        (3, "Carol", "35", "Chicago")
      ).toDF("id", "name", "age", "city")
        .write.mode("overwrite").saveAsTable("demo_customers")
      Thread.sleep(2000)

      // V5: Nullability change - id and name become NOT NULL (breaking)
      println("[V5] Making id and name non-nullable (BREAKING)")
      val v5Schema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", StringType, nullable = true),
        StructField("city", StringType, nullable = true)
      ))
      spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1, "Alice", "30", "New York"),
          Row(2, "Bob",   "25", "Boston")
        )),
        v5Schema
      ).write.mode("overwrite").saveAsTable("demo_customers")
      Thread.sleep(2000)

      // Summary
      println("\n" + "=" * 60)
      println("  SCHEMA EVOLUTION COMPLETE -- 5 versions captured")
      println("=" * 60)
      println("  V1: id, name, age              (initial)")
      println("  V2: +email, +city              (non-breaking add)")
      println("  V3: age INT->STRING            (breaking type change)")
      println("  V4: -email                     (breaking removal)")
      println("  V5: id/name NOT NULL           (breaking nullability)")
      println("")
      println("  Verify:")
      println("    curl http://localhost:8000/datasets/demo_customers/schema/history")
      println("    curl http://localhost:8000/datasets/demo_customers/schema")
      println("=" * 60)

    } catch {
      case e: Exception =>
        println(s"FAILED: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      Thread.sleep(2000)
      spark.stop()
    }
  }
}
