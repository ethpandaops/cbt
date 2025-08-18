#!/bin/bash
# Script to run the data generator with required dependencies

# Install Python dependencies
pip install clickhouse-connect

# Run the data generator
exec python /app/data-generator/generate.py