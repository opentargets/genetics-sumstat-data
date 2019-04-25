#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():

    # Args
    in_pattern = 'example_data/gwas/*.parquet'
    outf = 'output/gwas'
    pval_threshold = 0.05

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load
    df = spark.read.parquet(in_pattern)

    # Filter
    df = df.filter(col('pval') <= pval_threshold)
    
    # Repartition
    df = (
        df.repartitionByRange('chrom', 'pos')
        .orderBy('chrom', 'pos')
    )

    # Save
    (
        df
        .write.json(
            outf,
            mode='overwrite',
            compression='gzip'
        )
    )
    
    return 0



if __name__ == '__main__':

    main()
