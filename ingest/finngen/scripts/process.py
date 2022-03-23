#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Requires scipy and pandas

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
from time import time

import pyspark
import pyspark.sql
import pyspark.sql.functions
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from collections import OrderedDict
import scipy.stats as st
import argparse

num_partitions = 50

def main():

    # Parse args
    args = parse_args()
    args.min_mac = 10
    args.min_rows = 10000

    # Make spark session
    global spark

    if args.local:
        args.min_rows = 10000
        args.min_mac = 10
        args.n_cases = 8247
        args.n_total = 96499
        args.study_id = 'FINNGEN_AB1_INTESTINAL_INFECTIONS'
        args.in_sumstats = '/Users/jeremys/work/otgenetics/genetics-sumstat-data/ingest/finngen/example_data/finngen_R4_AB1_INTESTINAL_INFECTIONS.gz'
        args.out_parquet = '/Users/jeremys/work/otgenetics/genetics-sumstat-data/ingest/finngen/output/test.parquet'

        spark = (
            pyspark.sql.SparkSession.builder
                .config("parquet.enable.summary-metadata", "true")
                # .master('local[*]')
                .getOrCreate()
        )
    else:
        spark = (
            pyspark.sql.SparkSession.builder
                .config("parquet.enable.summary-metadata", "true")
                .getOrCreate()
        )

    print('args: ', args)
    print('Spark version: ', spark.version)
    start_time = time()

    # Load data
    data = load_sumstats(args.in_sumstats)

    # Drop NAs
    data = data.dropna(subset=['chrom', 'pos', 'ref', 'alt', 'pval', 'beta', 'se', 'eaf'])

    #
    # Stop if there are no few rows --------------------------------------------
    #

    data = data.persist()

    nrows = data.count()
    if nrows < args.min_rows:
        print('Skipping as only {0} rows in {1}'.format(nrows, args.in_sumstats))
        return 0

    #
    # Fill in other values and filter ------------------------------------------
    #

    # Add sample size, case numbers
    data = (
        data.withColumn('n_total', F.lit(args.n_total).cast(IntegerType()))
            .withColumn('n_cases', F.lit(args.n_cases).cast(IntegerType()))

    )

    # Calculate and filter based on MAC or MAC_cases
    data = (
        data.withColumn('maf', when(F.col('eaf') <= 0.5, col('eaf')).otherwise(1 - col('eaf')))
            .withColumn('mac', col('n_total') * 2 * col('maf'))
            .withColumn('mac_cases', col('n_cases') * 2 * col('maf'))
            .filter((col('mac') >= args.min_mac) & ((col('mac_cases') >= args.min_mac) | col('mac_cases').isNull()))
            .drop('maf')
    )
    data = data.persist()

    # If pval == 0.0, set to minimum float
    data = (
        data.withColumn('pval', when(col('pval') == 0.0,
                                     sys.float_info.min)
                                     .otherwise(col('pval')))
    )

    # Add study information columns
    data = (
        data.withColumn('type', lit('gwas'))
            .withColumn('study_id', lit(args.study_id))
            .withColumn('phenotype_id', lit(None).cast(StringType()))
            .withColumn('bio_feature', lit(None).cast(StringType()))
            .withColumn('gene_id', lit(None).cast(StringType()))
    )

    # Add is_cc column
    if args.n_cases is None:
        data = data.withColumn('is_cc', lit(False))
    else:
        data = data.withColumn('is_cc', lit(True))

    # Reorder all columns
    col_order = [
        'type',
        'study_id',
        'phenotype_id',
        'bio_feature',
        'gene_id',
        'chrom',
        'pos',
        'ref',
        'alt',
        'beta',
        'se',
        'pval',
        'n_total',
        'n_cases',
        'eaf',
        'mac',
        'mac_cases',
        'info',
        'is_cc'
    ]
    data = data.select(col_order)

    # Repartition - but I don't think we need to since we repartitioned
    # in load_sumstats.
    #data = (
    #    data.repartitionByRange('chrom', 'pos')
    #    .sortWithinPartitions('chrom', 'pos')
    #)

    # Write output
    (
        data.write.parquet(
            args.out_parquet,
            compression='snappy',
            mode='overwrite')
    )

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

def load_sumstats(inf):
    ''' Load a tsv sumstats file
    '''
    # Read
    df = ( spark.read.csv(inf,
                          sep='\t',
                          inferSchema=True,
                          header=True,
                          enforceSchema=False,
                          nullValue='NA') )

    # Calculate OR
    # print('calculating OR and CIs...')
    # .withColumn('oddsr', exp(col('beta')))
    # .withColumn('oddsr_lower', exp(col('beta') - 1.96 * col('sebeta')))
    df = (
        df.withColumnRenamed('#chrom', 'chrom')
            .withColumnRenamed('sebeta', 'se')
            .withColumnRenamed('af_alt', 'eaf')
            .withColumn('info', lit(None).cast(DoubleType()))
    )

    # Specify new names and types
    # ('odds_ratio', ('oddsr', DoubleType())),
    # ('ci_lower', ('oddsr_lower', DoubleType())),
    column_d = OrderedDict([
        ('chromosome', ('chrom', StringType())),
        ('base_pair_location', ('pos', IntegerType())),
        ('other_allele', ('ref', StringType())),
        ('effect_allele', ('alt', StringType())),
        ('p-value', ('pval', DoubleType())),
        ('beta', ('beta', DoubleType())),
        ('standard_error', ('se', DoubleType())),
        ('effect_allele_frequency', ('eaf', DoubleType())),
        ('info', ('info', DoubleType()))
    ])

    # Reorder columns
    selected_columns = [v[0] for _, v in column_d.items()]
    df = df.select(*selected_columns)

    # Repartition
    df = (
        df.repartitionByRange(num_partitions, 'chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    return df

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_sumstats', metavar="<file>", help=('Input sumstat file file'), type=str, required=False)
    parser.add_argument('--out_parquet', metavar="<file>", help=("Output file"), type=str, required=False)
    parser.add_argument('--study_id', metavar="<str>", help=("Study ID"), type=str, required=False)
    parser.add_argument('--n_total', metavar="<int>", help=("Total sample size"), type=int, required=False)
    parser.add_argument('--n_cases', metavar="<int>", help=("Number of cases"), type=int, required=False)
    parser.add_argument('--local', help="run local[*]", action='store_true', required=False, default=False)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
