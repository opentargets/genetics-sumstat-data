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
    pval_threshold = 0.005
    local = False
    
    if local:
        # # Args (local)
        in_pattern = 'example_data/gwas/*.parquet'
        outf = 'output/gwas_uncompressed'
    else:
        # Args (server)
        #in_completed = 'gs://genetics-portal-sumstats-b38/filtered/pvalue_0.05/gwas/190612' # Sumstats already filtered
        in_pattern = 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/*.parquet'
        outf = 'gs://genetics-portal-dev-sumstats/filtered/pvalue_0.005/gwas/{version}'.format(
            version=date.today().strftime("%y%m%d"))
    
    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    
    # Specify schema for GWAS studies - seems to be required to read
    # all parquet files from different directories
    gwas_schema = (
        StructType()
        .add('type', StringType(), False)
        .add('study_id', StringType(), False)
        .add('phenotype_id', StringType(), False)
        .add('bio_feature', StringType(), False)
        .add('gene_id', StringType(), False)
        .add('chrom', StringType(), True)
        .add('pos', IntegerType(), True)
        .add('ref', StringType(), True)
        .add('alt', StringType(), True)
        .add('beta', DoubleType(), True)
        .add('se', DoubleType(), True)
        .add('pval', DoubleType(), True)
        .add('n_total', IntegerType(), True)
        .add('n_cases', IntegerType(), True)
        .add('eaf', DoubleType(), True)
        .add('mac', DoubleType(), True)
        .add('mac_cases', DoubleType(), True)
        .add('info', DoubleType(), True)
        .add('is_cc', BooleanType(), True)
    )

    # Load
    #df = spark.read.parquet(in_pattern)
    df = (
        spark.read
        .schema(gwas_schema)
        .parquet(in_pattern)
#        .option("mergeSchema", "true")
    )
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
