#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
######## NOTE: This code is incomplete. We are still using SUN2018 sumstats
######## ingested using the older pipeline in the adjacent folder.

import sys
import os
import argparse
from time import time
import pandas as pd
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import scipy.stats as st
from collections import OrderedDict
from google.cloud import storage
from functools import reduce

def main():

    # Args
    args = parse_args()

    args.cis_dist = 1e6
    args.min_mac = 5
    print(args)

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.summary.metadata.level", "ALL")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    # TODO - specify schema before import, subset to only required columns

    # Load data
    df = (spark.read.csv(args.in_nominal,
                         sep='\t',
                         inferSchema=False,
                         enforceSchema=True,
                         header=True,
                         nullValue='NA'))
    nrows = sumstats.count()
    print("Number of rows read in: {}".format(nrows))
    #print(sumstats.show())
    print("Number of null values per column:")
    sumstats.select([count(when(isnull(c), c)).alias(c) for c in sumstats.columns]).show()

    # Add column with name of the input file
    get_filename = udf(lambda filename: filename.split('/')[-1])
    sumstats = df.withColumn('file_name', get_filename(input_file_name()))

    # Load mapping of filenames --> genes
    file_mapping = (
        spark.read.csv(args.in_filename_map,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=True)
        .select('file_name', 'gene_id')
        .dropna()
    )
    print(file_mapping.show())

    # Load ensembl id map
    ensembl_map = (
        spark.read.json(args.in_gene_meta)
        .select('gene_id', 'chr', 'tss')
        .withColumnRenamed('chr', 'chrom')
    )
    print(ensembl_map.show())
    
    gene_map = (
        file_mapping.join(ensembl_map, 'gene_id')
    )
    print(gene_map.show())

    # Join sumstats to associate gene and TSS with each input filename
    sumstats = sumstats.join(
        broadcast(gene_map),
        on=['file_name', 'chrom']
    )
    print(sumstats.show())

    # Filter to only keep variants within cis_dist of gene tss
    sumstats = sumstats.filter(
        abs(col('pos') - col('tss')) <= args.cis_dist
    )
    print(sumstats.show())

    # Drop uneeded cols
    sumstats = (
        sumstats
        .drop('file_name', 'tss')
    )

    sumstats = sumstats.persist()

    # Determine the number of variants tested per gene
    num_tests = (
        sumstats
        .groupby('gene_id')
        .agg(count(col('pval')).alias('num_tests'))
    )

    # Merge num_tests back onto nominal data
    sumstats = sumstats.join(num_tests, on=['gene_id'])
    
    # Additional columns to match gwas sumstat files
    sumstats = (
        sumstats.withColumn('study_id', lit(args.study_id))
            .withColumn('type', lit('pqtl'))
            .withColumn('phenotype_id', col('gene_id'))
            .withColumn('bio_feature', lit(args.bio_feature))
            .withColumn('n_total', lit(args.sample_size).cast('int'))
            .withColumn('n_cases', lit(None).cast('int'))
            .withColumn('info', lit(None).cast('double'))
            .withColumn('is_cc', lit(False))
            .withColumn('maf', when(col('eaf') > 0.5, 1 - col('eaf')).otherwise(col('eaf')))
            .withColumn('mac', (col('n_total') * 2 * col('maf')).cast('int'))
            .withColumn('mac_cases', lit(None).cast('int'))
            # Filter based on mac
            .filter(col('mac') >= args.min_mac)
    )

    # Re-order columns
    col_order = [
        'type',
        'study_id',
        'bio_feature',
        'phenotype_id',
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
        'num_tests',
        'info',
        'is_cc'
    ]
    sumstats = sumstats.select(col_order)

    # Drop NA rows
    required_cols = ['type', 'study_id', 'phenotype_id', 'bio_feature',
                     'gene_id', 'chrom', 'pos', 'ref', 'alt', 'beta', 'se', 'pval']
    sumstats = sumstats.dropna(subset=required_cols)
    print("Number of rows after filtering: {}".format(sumstats.count()))

    # Repartition and sort
    sumstats = (
        sumstats.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Write output
    (
        sumstats
        .write
        .partitionBy('bio_feature', 'chrom')
        .parquet(
            args.out_parquet,
            mode='overwrite',
            compression='snappy'
        )
    )

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0


def load_sumstats(in_pattern):
    ''' Loads harmonised pQTL genome-wide sumstats to spark df
    '''
    df = (spark.read.csv(in_pattern,
                         sep='\t',
                         inferSchema=False,
                         enforceSchema=True,
                         header=True,
                         nullValue='NA'))
    # Add column with name of the input file
    get_filename = udf(lambda filename: filename.split('/')[-1])
    df = df.withColumn('file_name', get_filename(input_file_name()))

    # Specify new names and types
    column_d = OrderedDict([
        ('hm_chrom', ('chrom', StringType())),
        ('hm_pos', ('pos', IntegerType())),
        ('hm_other_allele', ('ref', StringType())),
        ('hm_effect_allele', ('alt', StringType())),
        ('p_value', ('pval', DoubleType())),
        ('hm_beta', ('beta', DoubleType())),
        ('standard_error', ('se', DoubleType())),
        ('hm_effect_allele_frequency', ('eaf', DoubleType())),
        ('file_name', ('file_name', StringType()))
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

    return df


class ArgsPlaceholder():
    pass

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--study_id', metavar="<string>", help=('Study ID to add as column'), type=str, required=True)
    parser.add_argument('--sample_size', metavar="<int>", help=('Sample size of the study'), type=int, required=True)
    parser.add_argument('--bio_feature', metavar="<string>", help=('Used as bio_feature column'), type=str, required=True)
    parser.add_argument('--in_nominal', metavar="<file>", help=('Input sum stats'), type=str, required=True)
    parser.add_argument('--in_filename_map', metavar="<file>", help=('Path to file mapping sumstat filenames to gene IDs'), type=str, required=True)
    parser.add_argument('--in_gene_meta', metavar="<file>", help=("Input gene meta-data"), type=str, required=True)
    parser.add_argument('--out_parquet', metavar="<file>", help=("Output parquet path"), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()