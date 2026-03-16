package com.observability.examples

import org.apache.spark.sql.SparkSession

/**
 * Comprehensive test for column-level lineage extraction
 *
 * Tests the following scenarios that previously had issues:
 * 1. SELECT * (implicit column propagation)
 * 2. JOIN operations with implicit columns
 * 3. Mix of explicit and implicit transformations
 *
 * Run with:
 *   API_URL=https://data-observability-api-3ilve7g5cq-uc.a.run.app \
 *   API_KEY=your_key \
 *   ./run-column-lineage-test.sh
 */
object ColumnLineageTest {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("Column Lineage Test - SELECT *, JOINs, Implicit Propagation")
    println("=" * 70)
    println()

    val spark = SparkSession.builder()
      .appName("ColumnLineageTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // TEST 1: Create source tables
    println("Creating source tables...")

    val users = Seq(
      (1, "alice@example.com", "Alice", "US", 25),
      (2, "bob@example.com", "Bob", "GB", 30),
      (3, "carol@example.com", "Carol", "US", 28),
      (4, "dave@example.com", "Dave", "IN", 35),
      (5, "eve@example.com", "Eve", "GB", 22)
    ).toDF("user_id", "email", "name", "country", "age")

    val purchases = Seq(
      (1, 1, 100.0, "2025-01-15"),
      (2, 1, 50.0, "2025-01-16"),
      (3, 2, 200.0, "2025-01-15"),
      (4, 3, 75.0, "2025-01-16"),
      (5, 4, 300.0, "2025-01-17")
    ).toDF("purchase_id", "user_id", "amount", "purchase_date")

    // Write source tables
    users.write.mode("overwrite").parquet("/tmp/observability-test/users")
    purchases.write.mode("overwrite").parquet("/tmp/observability-test/purchases")

    println("✓ Source tables created")
    println()

    // TEST 2: SELECT * (implicit propagation) - Previously failed!
    println("Test 1: SELECT * (implicit column propagation)")
    println("-" * 70)

    val usersRead = spark.read.parquet("/tmp/observability-test/users")

    // This should capture lineage for ALL columns (user_id, email, name, country, age)
    // Previously only captured 0-1 columns!
    usersRead
      .selectExpr("*")
      .write
      .mode("overwrite")
      .parquet("/tmp/observability-test/users_copy")

    println("✓ Expected: 5 columns with DIRECT lineage")
    println("  (user_id, email, name, country, age)")
    println()

    // TEST 3: JOIN with SELECT * (previously only captured join key!)
    println("Test 2: JOIN with SELECT * (implicit + join key)")
    println("-" * 70)

    val usersForJoin = spark.read.parquet("/tmp/observability-test/users")
    val purchasesForJoin = spark.read.parquet("/tmp/observability-test/purchases")

    // This should capture:
    // - user_id: JOIN transform (used in join condition)
    // - All other columns: DIRECT transform (implicit propagation)
    // Previously only captured user_id!
    usersForJoin
      .join(purchasesForJoin, "user_id")
      .selectExpr("*")
      .write
      .mode("overwrite")
      .parquet("/tmp/observability-test/user_purchases")

    println("✓ Expected: 9 columns total")
    println("  - user_id: JOIN (from both tables)")
    println("  - email, name, country, age: DIRECT (from users)")
    println("  - purchase_id, amount, purchase_date: DIRECT (from purchases)")
    println()

    // TEST 4: Mix of explicit and implicit
    println("Test 3: Mix of explicit transformations and SELECT *")
    println("-" * 70)

    val userPurchases = spark.read.parquet("/tmp/observability-test/user_purchases")

    // This has:
    // - Explicit: email_upper (EXPRESSION)
    // - Implicit: all other columns via SELECT *
    // Previously might miss the implicit columns!
    userPurchases
      .selectExpr(
        "*",  // Implicit: all existing columns
        "UPPER(email) as email_upper"  // Explicit: new calculated column
      )
      .write
      .mode("overwrite")
      .parquet("/tmp/observability-test/user_purchases_enhanced")

    println("✓ Expected: 10 columns total")
    println("  - 9 columns: DIRECT (implicit from SELECT *)")
    println("  - email_upper: EXPRESSION (UPPER(email))")
    println()

    // TEST 5: Aggregation with GROUP BY (already worked, but verify)
    println("Test 4: Aggregation with GROUP BY (verification)")
    println("-" * 70)

    val userPurchasesAgg = spark.read.parquet("/tmp/observability-test/user_purchases")

    userPurchasesAgg
      .groupBy("country")
      .agg(
        org.apache.spark.sql.functions.count("*").as("purchase_count"),
        org.apache.spark.sql.functions.sum("amount").as("total_amount"),
        org.apache.spark.sql.functions.avg("amount").as("avg_amount")
      )
      .write
      .mode("overwrite")
      .parquet("/tmp/observability-test/country_stats")

    println("✓ Expected: 4 columns")
    println("  - country: DIRECT (GROUP BY key)")
    println("  - purchase_count: AGGREGATE (COUNT(*))")
    println("  - total_amount: AGGREGATE (SUM(amount))")
    println("  - avg_amount: AGGREGATE (AVG(amount))")
    println()

    // TEST 6: Multi-hop lineage (verify end-to-end)
    println("Test 5: Multi-hop lineage (users → user_purchases → country_stats)")
    println("-" * 70)

    println("✓ Lineage chain created:")
    println("  users.country → user_purchases.country → country_stats.country")
    println("  purchases.amount → user_purchases.amount → country_stats.total_amount")
    println()

    // Summary
    println("=" * 70)
    println("Test Complete!")
    println("=" * 70)
    println()
    println("Next steps:")
    println("1. Check API logs for column lineage ingestion")
    println("2. Query database:")
    println("   SELECT target_dataset_id, target_column, transform_type, COUNT(*)")
    println("   FROM column_lineage_edges")
    println("   GROUP BY 1, 2, 3;")
    println()
    println("3. Check frontend at:")
    println("   http://localhost:5173/column-lineage")
    println()
    println("Expected improvements:")
    println("  - users_copy: 5 edges (was 0-1)")
    println("  - user_purchases: 9 edges (was 1)")
    println("  - user_purchases_enhanced: 10 edges (was 1)")
    println("  - country_stats: 4 edges (was 4, should still work)")
    println()

    spark.stop()
  }
}
