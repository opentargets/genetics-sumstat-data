#!/usr/bin/env bash
#

set -euo pipefail

# Download
wget https://www.dropbox.com/s/puxks683vb0omeg/variants.tsv.bgz\?dl\=0 -O variants.tsv.bgz

# Unzip
zcat < variants.tsv.bgz > variants.tsv

# Convert to parquet
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
python to_parquet.py

# Tidy
rm variants.tsv
rm variants.tsv.bgz

echo COMPLETE
