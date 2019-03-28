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

    # Select rows that have "significant" p-values
    if args.data_type == 'gwas':
        sig = df.filter(col('pval') <= args.pval)
    elif args.data_type == 'moltrait':
        sig = df.filter(col('pval') <= (0.05 / col('num_tests')))
    sig = (
        sig
        .select('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos')
    )
    
    # Rename columns so that they can be removed after merge
    dup_cols = ['study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos']
    for colname in dup_cols:
        sig = sig.withColumnRenamed(colname, 'dup_' + colname)

    # Merge back onto main table
    merged = (
        df.alias('main').join(sig.alias('sig'),
        (
            (col('main.study_id') == col('sig.dup_study_id')) &
            (col('main.phenotype_id').eqNullSafe(col('sig.dup_phenotype_id'))) &
            (col('main.bio_feature').eqNullSafe(col('sig.dup_bio_feature'))) &
            (col('main.chrom') == col('sig.dup_chrom')) &
            (abs(col('main.pos') - col('sig.dup_pos')) <= args.window)
        ), how='inner'
    ))

    # Remove duplicated columns
    for colname in dup_cols:
        merged = merged.drop('dup_' + colname)
    
    # Remove duplicated rows
    merged = merged.distinct()
    
    # Repartition
    merged = (
        merged.repartitionByRange('chrom', 'pos')
            .orderBy('chrom', 'pos', 'ref', 'alt')
    )

    # Write output
    if args.data_type == 'gwas':
        (
            merged
            .write.parquet(
                args.out_sumstats,
                mode='overwrite',
                compression='snappy'
            )
        )
    elif args.data_type == 'moltrait':
        (
            merged
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
