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
import pandas as pd
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
        # .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load
    df = (
        spark.read.parquet(args.in_sumstats)
        .withColumn('chrom', col('chrom').cast('string'))
    )
    # # Load
    # df = (
    #     spark.read.csv(
    #         path=args.in_sumstats,
    #         sep='\t',
    #         inferSchema=True,
    #         header=True,
    #         nullValue='NA',
    #         comment='#')
    # )
    # df = df.withColumn('chrom', col('chrom').cast('string'))

    # Select rows that have "significant" p-values
    if args.data_type == 'gwas':
        sig = df.filter(col('pval') <= args.pval)
    elif args.data_type == 'moltrait':
        sig = df.filter(col('pval') <= (0.05 / col('num_tests')))
    sig = (
        sig
        .select('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos')
    )

    # Create intervals to keep based on specified window. Overlapping intervals
    # are merged to increase efficiency of join below
    intervals = create_intervals_to_keep(sig, window=args.window)

    # Join main table to intervals to keep with semi left join
    merged = (
        df.alias('main').join(broadcast(intervals.alias('intervals')),
        (
            (col('main.study_id') == col('intervals.study_id')) &
            (col('main.phenotype_id').eqNullSafe(col('intervals.phenotype_id'))) &
            (col('main.bio_feature').eqNullSafe(col('intervals.bio_feature'))) &
            (col('main.chrom') == col('intervals.chrom')) &
            (col('main.pos') >= col('intervals.start')) &
            (col('main.pos') <= col('intervals.end'))
        ), how='leftsemi'
    ))

    # Repartition
    merged = (
        merged.repartitionByRange('chrom', 'pos')
        .orderBy('chrom', 'pos')
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

    # # Write result as tsv
    # (
    #     merged
    #     .coalesce(1)
    #     .orderBy('study_id', 'chrom', 'pos')
    #     .write.csv(args.out_sumstats.replace('.parquet', '.tsv'), header=True, mode='overwrite')
    # )
    
    return 0

def create_intervals_to_keep(df, window):
    ''' Creates merged intervals from the significant positions
    '''

    # Create interval column
    intervals = (
        df.withColumn('interval', array(
            col('pos') - window, col('pos') + window))
          .drop('pos')
    )

    # Merge intervals
    merged_intervals = (
        intervals
        .groupby('study_id', 'phenotype_id', 'bio_feature', 'chrom')
        .apply(merge_intervals)
        .withColumn('start', when(col('start') > 0, col('start')).otherwise(0))
    )

    return merged_intervals

# Create return schema for merge function
ret_schema = (
    StructType()
    .add('study_id', StringType())
    .add('phenotype_id', StringType())
    .add('bio_feature', StringType())
    .add('chrom', StringType())
    .add('start', IntegerType())
    .add('end', IntegerType())
)

@pandas_udf(ret_schema, PandasUDFType.GROUPED_MAP)
def merge_intervals(key, pdf):
    ''' Merge overlapping intervals
    Params:
        key (list of group keys)
        pdf (pd.Df): input data
    Returns:
        pd.df of type ret_schema
    '''
    # Create list of intervals
    intervals = (
        pdf['interval']
        .apply(lambda x: Interval(s=x[0], e=x[1]))
        .tolist()
    )

    # Sort intervals
    intervals = sorted(intervals, key=lambda x: x.start)

    # Merge overlapping intervals
    result = [intervals[0]]
    for cur in intervals:
        if cur.start > result[-1].end:
            result.append(cur)
        elif result[-1].end < cur.end:
            result[-1].end = cur.end
    
    # Convert back into a df
    res_df_rows = []
    for interval in result:
        res_df_rows.append(
            list(key) + [interval.start, interval.end]
        )
    res_df = pd.DataFrame(
        res_df_rows,
        columns=['study_id', 'phenotype_id', 'bio_feature', 'chrom', 'start', 'end']
    )

    return res_df

# Interval type
class Interval(object):
    def __init__(self, s=0, e=0):
        self.start = s
        self.end = e

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
                   help=("pval threshold in window. Only used if --data_type gwas"),
                   metavar="<float>", type=float, required=True)
    p.add_argument('--data_type',
                   help=("Whether dataset is of GWAS or molecular trait type"),
                   metavar="<str>", type=str, choices=['gwas', 'moltrait'], required=True)

    args = p.parse_args()

    return args
    

if __name__ == '__main__':

    main()
