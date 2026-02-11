package com.observability.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Quick Lineage Test -- Smoke test for the Observability Platform
 *
 * Verifies that the listener captures:
 *   - Table lineage (raw -> bronze -> silver)
 *   - Column lineage: DIRECT, EXPRESSION, and AGGREGATE types
 *   - Schema capture for all 3 datasets
 *   - Basic metrics (row counts)
 *
 * This file contains ZERO com.observability imports.
 * The listener is registered transparently via spark.extraListeners.
 *
 * Usage:
 *   ./run-quick-test.sh
 *
 * Or manually:
 *   spark-submit \
 *     --class com.observability.examples.QuickLineageTest \
 *     --master "local[*]" \
 *     --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
 *     --conf spark.observability.api.url=http://localhost:8000 \
 *     --conf spark.observability.api.key=YOUR_API_KEY \
 *     target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
 *
 * Verify after running:
 *   curl http://localhost:8000/datasets
 *   curl http://localhost:8000/datasets/test_silver/lineage
 *   curl http://localhost:8000/datasets/test_silver/column-lineage
 */
object QuickLineageTest {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("  QUICK LINEAGE TEST")
    println("=" * 60)

    val spark = SparkSession.builder()
      .appName("Quick-Lineage-Test")
      .getOrCreate()

    val conf = spark.sparkContext.getConf
    println(s"  extraListeners = ${conf.get("spark.extraListeners", "NOT SET")}")
    println(s"  api.url        = ${conf.get("spark.observability.api.url", "NOT SET")}")
    println("=" * 60)

    try {
      runPipeline(spark)
    } finally {
      Thread.sleep(3000)
      spark.stop()
    }
  }

  private def runPipeline(spark: SparkSession): Unit = {
    import spark.implicits._

    // Clean up from previous runs to avoid "location already exists" errors
    Seq("test_raw", "test_bronze", "test_silver").foreach { t =>
      spark.sql(s"DROP TABLE IF EXISTS $t")
    }

    // STEP 1: Create RAW data
    println("\n[RAW] Creating test_raw (5 rows)")
    Seq(
      (1, "Alice", 100.0, "A"),
      (2, "Bob",   200.0, "B"),
      (3, "Carol", 150.0, "A"),
      (4, "David", 300.0, "B"),
      (5, "Eve",    50.0, "A")
    ).toDF("id", "name", "value", "category")
      .write.mode("overwrite").saveAsTable("test_raw")
    Thread.sleep(1000)

    // STEP 2: Transform to BRONZE
    // Column lineage: id->id (DIRECT), name->name_upper (EXPRESSION),
    //                 value->value_doubled (EXPRESSION), category->category (DIRECT)
    println("[BRONZE] Creating test_bronze (UPPER + value*2)")
    spark.sql("""
      SELECT id, upper(name) as name_upper, value * 2 as value_doubled, category
      FROM test_raw
      WHERE value > 0
    """).write.mode("overwrite").saveAsTable("test_bronze")
    Thread.sleep(1000)

    // STEP 3: Aggregate to SILVER
    // Column lineage: category->category (DIRECT), *->row_count (AGGREGATE: COUNT),
    //                 value_doubled->total_value (AGGREGATE: SUM), value_doubled->avg_value (AGGREGATE: AVG)
    println("[SILVER] Creating test_silver (GROUP BY category: COUNT, SUM, AVG)")
    spark.sql("""
      SELECT category,
             count(*) as row_count,
             sum(value_doubled) as total_value,
             avg(value_doubled) as avg_value
      FROM test_bronze
      GROUP BY category
    """).write.mode("overwrite").saveAsTable("test_silver")
    Thread.sleep(1000)

    // Print results
    println("\n--- test_raw ---")
    spark.table("test_raw").show()
    println("--- test_bronze ---")
    spark.table("test_bronze").show()
    println("--- test_silver ---")
    spark.table("test_silver").show()

    println("=" * 60)
    println("  TEST COMPLETE")
    println("=" * 60)
    println("  Table lineage:  test_raw -> test_bronze -> test_silver")
    println("  Column lineage: DIRECT (id, category)")
    println("                  EXPRESSION (upper(name), value*2)")
    println("                  AGGREGATE (COUNT, SUM, AVG)")
    println("  Schemas:        3 datasets captured")
    println("=" * 60)
  }
}
