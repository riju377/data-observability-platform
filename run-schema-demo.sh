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
#   API_KEY  - API key for authentication (or prompted interactively)
#   API_URL  - Override backend URL (optional, defaults to Render)
# ============================================================

set -e

PACKAGE="io.github.riju377:data-observability-platform_2.12:1.4.0"

echo "=============================================="
echo " Schema Evolution Demo"
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
echo "  API Key: ${API_KEY:0:12}..."
echo ""

# Clean up leftover Spark artifacts from previous runs
rm -rf spark-warehouse/demo_customers metastore_db derby.log

spark-submit \
  --packages "$PACKAGE" \
  --class com.observability.examples.SchemaEvolutionDemo \
  --master "local[*]" \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf "spark.observability.api.key=${API_KEY}" \
  ${API_URL:+--conf "spark.observability.api.url=${API_URL}"} \
  dummy.jar

echo ""
echo "=============================================="
echo " Done! Verify results:"
echo "=============================================="
echo ""
echo "  # View schema history (5 versions)"
echo "  curl 'https://data-observability-api.onrender.com/datasets/demo_customers/schema/history'"
echo ""
echo "  # View current schema"
echo "  curl 'https://data-observability-api.onrender.com/datasets/demo_customers/schema'"
echo ""
echo "  # Or view in the UI at https://data-observability.vercel.app"
echo ""
