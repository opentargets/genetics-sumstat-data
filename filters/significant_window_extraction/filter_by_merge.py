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

    # Run filtering
    filter_significant_windows(
        in_pq=args.in_sumstats,
        out_pq=args.out_sumstats,
        data_type=args.data_type,
        window=args.window,
        pval=args.pval
    )
    
    return 0


def filter_significant_windows(in_pq, out_pq, data_type, window, pval):
    ''' Filters a sumstat parquet down to windows that contain a "significant"
        variant, as determined by pval
    Args:
        in_pq (path): input parquet
        out_pq (path): output parquet
        data type (str): gwas or moltrait
        window (int): window to extract around significant variants
        pval (float): pvalue to be considered significant
    '''

    # Load
    df = (
        spark.read.parquet(in_pq)
        .withColumn('chrom', col('chrom').cast('string'))
    )

    # Select rows that have "significant" p-values
    if data_type == 'gwas':
        sig = df.filter(col('pval') <= pval)
    elif data_type == 'moltrait':
        sig = df.filter(col('pval') <= (0.05 / col('num_tests')))
    sig = (
        sig
        .select('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos')
    )

    # Create intervals to keep based on specified window. Overlapping intervals
    # are merged to increase efficiency of join below
    intervals = create_intervals_to_keep(sig, window=window)

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
        .sortWithinPartitions('chrom', 'pos')
    )

    # Write output
    if data_type == 'gwas':
        (
            merged
            .write.parquet(
                out_pq,
                mode='overwrite',
                compression='snappy'
            )
        )
    elif data_type == 'moltrait':
        (
            merged
            .write
            .partitionBy('bio_feature', 'chrom')
            .parquet(
                out_pq,
                mode='overwrite',
                compression='snappy'
            )
        )

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
