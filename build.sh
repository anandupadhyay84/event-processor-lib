#!/bin/bash

# Build script for Event Processor Library
# Requires: Java 17+, Maven 3.8+

set -e

echo "======================================"
echo "Event Processor Library Build"
echo "======================================"

echo "Java version:"
java -version

echo ""
echo "Maven version:"
mvn -version

echo ""
echo "Building project..."
mvn clean verify

echo ""
echo "======================================"
echo "Build completed successfully!"
echo "Artifact:"
echo "  target/event-processor-lib-1.0.0.jar"
echo "======================================"