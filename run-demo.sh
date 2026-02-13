#!/bin/bash

# ============================================================
# Full Pipeline Demo with Anomaly Detection + Alerting
# ============================================================
# Runs FullPipelineDemo -- the comprehensive showcase that tests:
#   - Medallion architecture (Raw -> Bronze -> Silver -> Gold)
#   - Diamond lineage pattern
#   - Multi-source ingestion (table, parquet, CSV)
#   - Column lineage (DIRECT, EXPRESSION, AGGREGATE, JOIN)
#   - Schema capture
#   - Anomaly detection (RowCountSpike via 3-sigma)
#   - Alert triggering
#
# This script:
#   1. Creates an alert rule for the demo (via API)
#   2. Runs the full pipeline demo
#   3. Prints verification commands
#
# Configuration via environment variables:
#   API_KEY  - API key for authentication (or prompted interactively)
#   API_URL  - Override backend URL (optional, defaults to Render)
# ============================================================

set -e

API_URL="${API_URL:-https://data-observability-api.onrender.com}"
PACKAGE="io.github.riju377:data-observability-platform_2.12:1.3.0"

echo "=============================================="
echo " Full Pipeline Demo"
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

# Step 1: Create alert rule for the demo
echo "Setting up alert rule for anomaly detection..."
curl -s -X POST "${API_URL}/alert-rules" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Demo Spike Alert",
    "description": "Alert on row count spikes in bronze_taxi_trips (created by run-demo.sh)",
    "dataset_name": "bronze_taxi_trips",
    "severity": "WARNING",
    "channel_type": "EMAIL",
    "channel_config": {"email_to": ["demo@example.com"]},
    "enabled": true,
    "deduplication_minutes": 5
  }' > /dev/null 2>&1 && echo "  -> Alert rule created" || echo "  -> Alert rule may already exist (OK)"

echo ""
echo "Starting pipeline demo..."
echo ""

# Step 2: Clean up leftover Spark artifacts from previous runs
rm -rf spark-warehouse metastore_db derby.log

# Step 3: Run the full pipeline demo
spark-submit \
  --packages "$PACKAGE" \
  --class com.observability.examples.FullPipelineDemo \
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
echo "  # Check detected anomalies"
echo "  curl '${API_URL}/anomalies?dataset_name=bronze_taxi_trips&hours=1'"
echo ""
echo "  # Check metrics history"
echo "  curl '${API_URL}/datasets/bronze_taxi_trips/metrics?hours=1'"
echo ""
echo "  # Check alert history"
echo "  curl '${API_URL}/alert-history?hours=1'"
echo ""
echo "  # View table lineage"
echo "  curl '${API_URL}/datasets/silver_trip_weather/lineage'"
echo ""
echo "  # View column lineage"
echo "  curl '${API_URL}/datasets/gold_borough_metrics/column-lineage'"
echo ""
echo "  # Or view in the UI at https://data-observability.vercel.app"
echo ""
