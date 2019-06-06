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

    # # Args (local)
    # in_pattern = 'example_data/gwas/*.parquet'
    # outf = 'output/gwas_uncompressed'
    # pval_threshold = 0.05
 
    # Args (server)
    in_pattern = 'gs://genetics-portal-sumstats-b38/unfiltered/gwas/*.parquet'
    outf = 'gs://genetics-portal-sumstats-b38/filtered/pvalue_0.05/gwas/190606'
    pval_threshold = 0.05

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load
    df = spark.read.parquet(in_pattern)

    # Filter
    df = df.filter(col('pval') <= pval_threshold)

    # Rename type to type_id, and cast info to float
    df = (
        df.withColumnRenamed('type', 'type_id')
          .withColumn('info', col('info').cast(DoubleType()))
    )
    
    # # Repartition
    # df = (
    #     df.repartitionByRange('chrom', 'pos')
    #     .sortWithinPartitions('chrom', 'pos')
    # )

    # Save
    (
        df
        .write.parquet(
            outf,
            mode='overwrite'
        )
    )
    
    return 0



if __name__ == '__main__':

    main()
