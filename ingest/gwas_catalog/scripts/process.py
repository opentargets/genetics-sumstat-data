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
import os
from time import time
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from collections import OrderedDict
import scipy.stats as st
import argparse


def main():

    # Parse args
    args = parse_args()
    args.min_mac = 10
    args.min_rows = 10000

    print(args)

    # # Test args
    # args = ArgPlacehorder()
    # args.min_mac = 10
    # args.n_cases = 5000
    # args.n_total = 10000
    # args.study_id = 'STUDY_TEST'
    # args.in_sumstat = 'example_data/custom.tsv'
    # args.in_af = 'example_data/variant-annotation_af-only_chrom10.parquet'
    # args.out_parquet = 'output/test.parquet'

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    # Load data
    data = load_sumstats(args.in_sumstats)

    #
    # Fill in required missing values -----------------------------------------
    #

    # Replace beta/se with logOR/logORse if oddsr and oddsr_lower not null
    perc_97th = 1.95996398454005423552
    to_do = (data.oddsr.isNotNull() & data.oddsr_lower.isNotNull())
    data = (
        data.withColumn('beta', when(
            to_do, log(data.oddsr)).otherwise(data.beta))
        .withColumn('se', when(to_do, (log(data.oddsr) - log(data.oddsr_lower))/perc_97th).otherwise(data.se))
        .drop('oddsr', 'oddsr_lower')
    )

    # Impute standard error if missing
    to_do = (data.beta.isNotNull() & data.pval.isNotNull() & data.se.isNull())
    data = (
        data.withColumn('z_abs', abs(ppf_udf(col('pval'))))
        .withColumn('se', when(to_do, abs(col('beta')) / col('z_abs')).otherwise(col('se')))
        .drop('z_abs')
    )

    # Drop NAs, eaf null is ok as this will be inferred from a reference
    data = data.dropna(
        subset=['chrom', 'pos', 'ref', 'alt', 'pval', 'beta', 'se'])

    #
    # Stop if there are no few rows --------------------------------------------
    #

    data = data.persist()

    nrows = data.count()
    if nrows < args.min_rows:
        print('Skpping as only {0} rows in {1}'.format(
            nrows, args.in_sumstats))
        return 0

    #
    # Fill in effect allele frequency using gnomad NFE frequency ---------------
    #

    # If there are any nulls in eaf, get allele freq from reference
    if data.filter(col('eaf').isNull()).count() > 0:

        # Load gnomad allele frequencies
        afs = (
            spark.read.parquet(args.in_af)
                 .select('chrom_b38', 'pos_b38', 'ref', 'alt', 'af.gnomad_nfe')
                 .withColumnRenamed('chrom_b38', 'chrom')
                 .withColumnRenamed('pos_b38', 'pos')
                 .dropna()
        )

        # Join
        data = data.join(afs, on=['chrom', 'pos', 'ref', 'alt'], how='left')

        # Make fill in blanks on the EAF column using gnomad AF
        data = (
            data.withColumn('eaf', when(col('eaf').isNull(),
                                        col('gnomad_nfe'))
                            .otherwise(col('eaf')))
            .drop('gnomad_nfe')
        )

    # Drop rows without effect allele frequency
    data = data.dropna(subset=['eaf'])

    #
    # Fill in other values and filter ------------------------------------------
    #

    # args.n_cases = None

    # Add sample size, case numbers
    data = (
        data.withColumn('n_total', lit(args.n_total).cast(IntegerType()))
            .withColumn('n_cases', lit(args.n_cases).cast(IntegerType()))

    )

    # Calculate and filter based on MAC or MAC_cases
    data = (
        data.withColumn('maf', when(col('eaf') <= 0.5,
                                    col('eaf')).otherwise(1 - col('eaf')))
        .withColumn('mac', col('n_total') * 2 * col('maf'))
        .withColumn('mac_cases', col('n_cases') * 2 * col('maf'))
        .filter((col('mac') >= args.min_mac) & ((col('mac_cases') >= args.min_mac) | col('mac_cases').isNull()))
        .drop('maf')
    )

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

    # Repartition
    data = (
        data.repartitionByRange('chrom', 'pos')
            .orderBy('chrom', 'pos', 'ref', 'alt')
    )

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
    ''' Load a harmonised GWAS Catalog file
    '''
    # Read
    df = (spark.read.csv(inf,
                         sep='\t',
                         inferSchema=False,
                         enforceSchema=True,
                         header=True,
                         nullValue='NA'))

    # Specify new names and types
    column_d = OrderedDict([
        ('hm_chrom', ('chrom', StringType())),
        ('hm_pos', ('pos', IntegerType())),
        ('hm_other_allele', ('ref', StringType())),
        ('hm_effect_allele', ('alt', StringType())),
        ('p_value', ('pval', DoubleType())),
        ('hm_beta', ('beta', DoubleType())),
        ('standard_error', ('se', DoubleType())),
        ('hm_odds_ratio', ('oddsr', DoubleType())),
        ('hm_ci_lower', ('oddsr_lower', DoubleType())),
        ('hm_effect_allele_frequency', ('eaf', DoubleType())),
        ('info', ('info', DoubleType()))
    ])

    # Add missing columns as null
    for column in column_d.keys():
        if column not in df.columns:
            df = df.withColumn(column, lit(None).cast(column_d[column][1]))

    # Reorder columns
    df = df.select(*list(column_d.keys()))

    # Change type and name of all columns
    for column in column_d.keys():
        df = (df.withColumn(column, col(column).cast(column_d[column][1]))
              .withColumnRenamed(column, column_d[column][0]))

    # If "low_confidence_variant" exists, filter based on it
    if 'low_confidence_variant' in df.columns:
        df = df.filter(~col('low_confidence_variant'))

    # Repartition
    df = df.repartitionByRange('chrom', 'pos')

    return df


def ppf(pval):
    ''' Return inverse cumulative distribution function of the normal
        distribution. Needed to calculate stderr.
    '''
    return float(st.norm.ppf(pval / 2))


ppf_udf = udf(ppf, DoubleType())


def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_sumstats', metavar="<file>",
                        help=('Input sumstat file file'), type=str, required=True)
    parser.add_argument('--in_af', metavar="<file>",
                        help=('Input allele frequency parquet'), type=str, required=True)
    parser.add_argument('--out_parquet', metavar="<file>",
                        help=("Output file"), type=str, required=True)
    parser.add_argument('--study_id', metavar="<str>",
                        help=("Study ID"), type=str, required=True)
    parser.add_argument('--n_total', metavar="<int>",
                        help=("Total sample size"), type=int, required=True)
    parser.add_argument('--n_cases', metavar="<int>",
                        help=("Number of cases"), type=int, required=False)
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    main()
#
