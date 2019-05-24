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
import os
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def main():

    # Args
    in_data = '../example_data/input_test_dataset.tsv'
    out_res = '../output/merge_filter.test.tsv'
    p_threshold = 0.05
    dist_threshold = 10

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load
    df = (
        spark.read.csv(
            path=in_data,
            sep='\t',
            inferSchema=True,
            header=True,
            nullValue='NA',
            comment='#')
    )

    # Select rows that have "significant" p-values
    sig = (
        df.filter(col('pval') <= p_threshold)
        .select('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos')
    )

    # Merge back onto main table
    merged = (
        df.alias('main').join(sig.alias('sig'),
                              (
            (col('main.study_id') == col('sig.study_id')) &
            (col('main.phenotype_id').eqNullSafe(col('sig.phenotype_id'))) &
            (col('main.bio_feature').eqNullSafe(col('sig.bio_feature'))) &
            (col('main.chrom') == col('sig.chrom')) &
            (abs(col('main.pos') - col('sig.pos')) <= dist_threshold)
        ), how='leftsemi'
        ))

    # Remove duplicated rows
    merged = merged.distinct()

    # Repartition
    merged = (
        merged.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Write result
    (
        merged
        .coalesce(1)
        .orderBy('study_id', 'chrom', 'pos')
        .write.csv(out_res, header=True, mode='overwrite')
    )

    

if __name__ == '__main__':

    main()
