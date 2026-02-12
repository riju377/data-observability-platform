#!/bin/bash

# ============================================================
# Schema Evolution Demo
# ============================================================
# Runs SchemaEvolutionDemo -- writes 5 progressive schema versions
# to the same table (demo_customers) to test schema tracking:
#
#   V1: id, name, age              (initial)
#   V2: +email, +city              (non-breaking add)
#   V3: age INT->STRING            (breaking type change)
#   V4: -email                     (breaking removal)
#   V5: id/name NOT NULL           (breaking nullability)
#
# Configuration via environment variables:
#   API_URL  - Backend API URL (default: http://localhost:8000)
#   API_KEY  - API key for authentication
#   JAR_PATH - Path to assembly JAR
# ============================================================

set -e

API_URL="${API_URL:-https://data-observability-api.onrender.com}"
API_KEY="${API_KEY:-obs_live_CEsbjnPVpAPIRsBefSVVZ20zGh_dvoq73P8Z-GGB94A}"
JAR_PATH="${JAR_PATH:-target/scala-2.12/data-observability-platform-assembly-1.2.0.jar}"

echo "=============================================="
echo " Schema Evolution Demo"
echo "=============================================="
echo "  API URL: $API_URL"
echo "  JAR:     $JAR_PATH"
echo ""

# Check JAR exists
if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: JAR not found at $JAR_PATH"
  echo "Build it with: sbt assembly"
  exit 1
fi

# Clean up leftover Spark artifacts from previous runs
rm -rf spark-warehouse/demo_customers metastore_db derby.log

spark-submit \
  --class com.observability.examples.SchemaEvolutionDemo \
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
echo "  # View schema history (5 versions)"
echo "  curl '${API_URL}/datasets/demo_customers/schema/history'"
echo ""
echo "  # View current schema"
echo "  curl '${API_URL}/datasets/demo_customers/schema'"
echo ""
echo "  # Or view in the UI at http://localhost:5173"
echo ""
