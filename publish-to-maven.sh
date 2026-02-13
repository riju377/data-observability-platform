#!/bin/bash

# ============================================================
# Publish to Maven Central
# ============================================================
# Publishes the Spark listener JAR to Maven Central via
# Sonatype Central Portal.
#
# Prerequisites:
#   1. GPG key configured (gpg --list-keys should show your key)
#   2. Sonatype credentials in ~/.sbt/sonatype_central_credentials:
#        host=central.sonatype.com
#        user=<your token username>
#        password=<your token password>
#   3. sbt 1.11.0+ (for Central Portal support)
#
# Usage:
#   ./publish-to-maven.sh          # publish current version
#   ./publish-to-maven.sh 2.14.0   # bump to 2.14.0 and publish
# ============================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_FILE="$PROJECT_DIR/build.sbt"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${CYAN}=============================================="
echo " Maven Central Publisher"
echo -e "==============================================${NC}"
echo ""

# ---- Optional: Bump version ----
if [ -n "$1" ]; then
  NEW_VERSION="$1"
  CURRENT_VERSION=$(grep '^version :=' "$BUILD_FILE" | sed 's/version := "\(.*\)"/\1/')

  echo -e "${YELLOW}Bumping version: ${CURRENT_VERSION} → ${NEW_VERSION}${NC}"
  sed -i '' "s/^version := \".*\"/version := \"${NEW_VERSION}\"/" "$BUILD_FILE"
  echo -e "${GREEN}✓ build.sbt updated${NC}"
  echo ""
else
  NEW_VERSION=$(grep '^version :=' "$BUILD_FILE" | sed 's/version := "\(.*\)"/\1/')
fi

echo -e "  Version:      ${GREEN}${NEW_VERSION}${NC}"
echo -e "  Organization: io.github.riju377"
echo -e "  Artifact:     data-observability-platform_2.12"
echo ""

# ---- Pre-flight checks ----
echo -e "${CYAN}Running pre-flight checks...${NC}"

# Check sbt
if ! command -v sbt &> /dev/null; then
  echo -e "${RED}✗ sbt not found. Install it first.${NC}"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} sbt found"

# Check GPG
if ! gpg --list-secret-keys --keyid-format LONG 2>/dev/null | grep -q "sec"; then
  echo -e "${RED}✗ No GPG secret key found. Run: gpg --gen-key${NC}"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} GPG key found"

# Check credentials file
CREDS_FILE="$HOME/.sbt/sonatype_central_credentials"
if [ ! -f "$CREDS_FILE" ]; then
  echo -e "${RED}✗ Credentials not found at ${CREDS_FILE}${NC}"
  echo "  Create the file with:"
  echo "    host=central.sonatype.com"
  echo "    user=<your token username>"
  echo "    password=<your token password>"
  exit 1
fi
echo -e "  ${GREEN}✓${NC} Sonatype credentials found"
echo ""

# ---- Confirm ----
echo -e "${YELLOW}Ready to publish v${NEW_VERSION} to Maven Central.${NC}"
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  echo "Aborted."
  exit 0
fi
echo ""

# ---- Clean & Test ----
echo -e "${CYAN}Step 1/3: Clean & Test${NC}"
cd "$PROJECT_DIR"
sbt clean test
echo -e "${GREEN}✓ Tests passed${NC}"
echo ""

# ---- Publish Signed ----
echo -e "${CYAN}Step 2/3: Publish Signed Artifacts${NC}"
sbt publishSigned
echo -e "${GREEN}✓ Artifacts signed and staged${NC}"
echo ""

# ---- Release to Central ----
echo -e "${CYAN}Step 3/3: Release to Maven Central${NC}"
sbt sonaUpload
echo -e "${GREEN}✓ Release submitted to Maven Central${NC}"
echo ""

# ---- Done ----
echo -e "${GREEN}=============================================="
echo " ✅ Published successfully!"
echo "=============================================="
echo ""
echo -e "  ${NC}Artifact: io.github.riju377:data-observability-platform_2.12:${NEW_VERSION}"
echo ""
echo "  It may take 10-30 minutes to appear on Maven Central:"
echo "  https://central.sonatype.com/artifact/io.github.riju377/data-observability-platform_2.12"
echo ""
echo "  Maven:"
echo "    <dependency>"
echo "      <groupId>io.github.riju377</groupId>"
echo "      <artifactId>data-observability-platform_2.12</artifactId>"
echo "      <version>${NEW_VERSION}</version>"
echo "    </dependency>"
echo ""
echo "  SBT:"
echo "    libraryDependencies += \"io.github.riju377\" %% \"data-observability-platform\" % \"${NEW_VERSION}\""
echo ""
echo "  spark-submit:"
echo "    --packages io.github.riju377:data-observability-platform_2.12:${NEW_VERSION}"
echo ""
