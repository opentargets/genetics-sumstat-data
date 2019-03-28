#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Took 39 mins on 16 core dataproc

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
import argparse
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main():

    # Args
    args = parse_args()

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load
    df = (
        spark.read.parquet(args.in_sumstats)
        .withColumn('chrom', col('chrom').cast('string'))
    )

    # Make window spec
    window_spec = (
        Window
        .partitionBy('study_id', 'phenotype_id', 'bio_feature', 'chrom')
        .orderBy('pos')
        .rangeBetween(-args.window, args.window)
    )

    # Get minimum pvalue over window
    df = (
        df.withColumn('window_min_p', 
            min(col('pval')).over(window_spec)
        )
    )

    # Filter then drop window_min_p col
    if args.data_type == 'gwas':
        df_filt = df.filter(col('window_min_p') <= args.pval)
    elif args.data_type == 'moltrait':
        df_filt = df.filter(col('window_min_p') <= (0.05 / col('num_tests')))
    df_filt = df_filt.drop('window_min_p')
    
    # Repartition
    df_filt = (
        df_filt.repartitionByRange('chrom', 'pos')
            .orderBy('chrom', 'pos', 'ref', 'alt')
    )

    # Write output
    if args.data_type == 'gwas':
        (
            df_filt
            .write.parquet(
                args.out_sumstats,
                mode='overwrite',
                compression='snappy'
            )
        )
    elif args.data_type == 'moltrait':
        (
            df_filt
            .write
            .partitionBy('bio_feature', 'chrom')
            .parquet(
                args.out_sumstats,
                mode='overwrite',
                compression='snappy'
            )
        )
    
    return 0

def parse_args():
    ''' Load command line args.
    '''
    p = argparse.ArgumentParser()

    p.add_argument('--in_sumstats',
                   help=("Input: summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--out_sumstats',
                   help=("Output: summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--window',
                   help=("± window to filter by"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--pval',
                   help=("pval threshold in window. Only used if --data_type == gwas"),
                   metavar="<float>", type=float, required=True)
    p.add_argument('--data_type',
                   help=("Whether dataset is of GWAS or molecular trait type"),
                   metavar="<str>", type=str, choices=['gwas', 'moltrait'], required=True)

    args = p.parse_args()

    return args
    

if __name__ == '__main__':

    main()
