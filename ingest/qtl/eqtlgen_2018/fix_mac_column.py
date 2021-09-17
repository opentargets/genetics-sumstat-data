#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
# The MAC column for two datasets (eQTLGen and SUN2018) was saved as a double,
# but should have been an int, for compatibility with other QTL datasets
# ingested. This fixes that. 
import sys
import os
from time import time
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def main():

    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    df = spark.read.parquet('gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_tofix/eQTLGen.parquet')
    # Write back to main molecular_trait directory
    df = df.withColumn('mac', col('mac').cast(IntegerType()))
    (
        df
        .write
        .partitionBy('bio_feature', 'chrom')
        .parquet(
            'gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait/eQTLGen.parquet',
            mode='overwrite',
            compression='snappy'
        )
    )
    # Write to artificially partitioned molecular trait directory,
    # so that we can read all mol_trait studies at once
    df = df.withColumn('mac', col('mac').cast(IntegerType()))
    (
        df
        .write
        .partitionBy('bio_feature', 'chrom')
        .parquet(
            'gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_partitioned/col_study_id=eQTLGen/',
            mode='overwrite',
            compression='snappy'
        )
    )

    return 0

if __name__ == '__main__':

    main()
