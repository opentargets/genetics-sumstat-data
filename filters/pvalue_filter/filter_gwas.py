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
from datetime import date

def main():
    in_completed = None
    pval_threshold = 0.05
    local = False
    
    if local:
        # # Args (local)
        in_pattern = 'example_data/gwas/*.parquet'
        outf = 'output/gwas_uncompressed'
    else:
        # Args (server)
        in_completed = 'gs://genetics-portal-sumstats-b38/filtered/pvalue_0.05/gwas/190612' # Sumstats already filtered
        in_pattern = 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/*.parquet' # Newly ingested sumstats
        outf = 'gs://genetics-portal-dev-sumstats/filtered/pvalue_0.05/gwas/{version}'.format(
            version=date.today().strftime("%y%m%d"))
    
    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    
    # Load
    df = spark.read.parquet(in_pattern)
    if "phenotype_id" in df.columns:
        df = df.drop("phenotype_id", "bio_feature", "gene_id")
    
    # Filter
    df = df.filter(col('pval') <= pval_threshold)
    
    if in_completed is not None:
        # Read already filtered data
        df_completed = spark.read.parquet(in_completed)
        df = df_completed.unionByName(df) 
    
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
