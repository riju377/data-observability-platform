#!/bin/bash

# ============================================================
# Quick Lineage Test
# ============================================================
# Runs QuickLineageTest via spark-submit with the ObservabilityListener.
# The listener is registered transparently -- no code changes needed.
#
# Configuration via environment variables:
#   API_URL  - Backend API URL (default: http://localhost:8000)
#   API_KEY  - API key for authentication
#   JAR_PATH - Path to assembly JAR
# ============================================================

set -e

API_URL="${API_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-YOUR_API_KEY}"
JAR_PATH="${JAR_PATH:-target/scala-2.12/data-observability-platform-assembly-1.1.0.jar}"

echo "=============================================="
echo " Quick Lineage Test"
echo "=============================================="
echo "  API URL: $API_URL"
echo "  JAR:     $JAR_PATH"
echo ""
echo "  This test will:"
echo "    1. Create test_raw (5 rows)"
echo "    2. Transform to test_bronze (UPPER, value*2)"
echo "    3. Aggregate to test_silver (COUNT, SUM, AVG)"
echo "    4. Capture all lineage and schemas"
echo ""

# Check JAR exists
if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR not found at $JAR_PATH"
  echo "Build it with: sbt assembly"
  exit 1
fi

# Clean up leftover Spark artifacts from previous runs
rm -rf spark-warehouse/test_raw spark-warehouse/test_bronze spark-warehouse/test_silver metastore_db derby.log

spark-submit \
  --class com.observability.examples.QuickLineageTest \
  --master "local[*]" \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf "spark.observability.api.url=${API_URL}" \
  --conf "spark.observability.api.key=${API_KEY}" \
  "$JAR_PATH"

echo ""
echo "=============================================="
echo " Done! Verify results:"
echo "=============================================="
echo ""
echo "  # List all datasets"
echo "  curl ${API_URL}/datasets"
echo ""
echo "  # View table lineage"
echo "  curl ${API_URL}/datasets/test_silver/lineage"
echo ""
echo "  # View column lineage"
echo "  curl ${API_URL}/datasets/test_silver/column-lineage"
echo ""
echo "  # Or view in the UI at http://localhost:3000"
echo ""
