#!/bin/bash

# ============================================================
# Quick Lineage Test
# ============================================================
# Runs QuickLineageTest via spark-submit with the ObservabilityListener.
# The listener is registered transparently -- no code changes needed.
#
# Configuration via environment variables:
#   API_KEY  - API key for authentication (or prompted interactively)
#   API_URL  - Override backend URL (optional, defaults to Render)
# ============================================================

set -e

API_URL="${API_URL:-https://data-observability-api.onrender.com}"
PACKAGE="io.github.riju377:data-observability-platform_2.12:2.13.0"

echo "=============================================="
echo " Quick Lineage Test"
echo "=============================================="
echo ""

# ---- API Key ----
if [ -z "$API_KEY" ]; then
  echo "  An API key is required to send data to the platform."
  echo ""
  echo "  Don't have one? Create it from the dashboard:"
  echo "  → https://data-observability.vercel.app/api-keys"
  echo ""
  read -p "  Enter your API key: " API_KEY
  echo ""

  if [ -z "$API_KEY" ]; then
    echo "  ✗ No API key provided. Exiting."
    exit 1
  fi
fi

echo "  Package: $PACKAGE"
echo "  API URL: $API_URL"
echo "  API Key: ${API_KEY:0:12}..."
echo ""
echo "  This test will:"
echo "    1. Create test_raw (5 rows)"
echo "    2. Transform to test_bronze (UPPER, value*2)"
echo "    3. Aggregate to test_silver (COUNT, SUM, AVG)"
echo "    4. Capture all lineage and schemas"
echo ""

# Clean up leftover Spark artifacts from previous runs
rm -rf spark-warehouse/test_raw spark-warehouse/test_bronze spark-warehouse/test_silver metastore_db derby.log

spark-submit \
  --packages "$PACKAGE" \
  --class com.observability.examples.QuickLineageTest \
  --master "local[*]" \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf "spark.observability.api.key=${API_KEY}" \
  --conf "spark.observability.api.url=${API_URL}" \
  dummy.jar

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
echo "  # Or view in the UI at https://data-observability.vercel.app"
echo ""
