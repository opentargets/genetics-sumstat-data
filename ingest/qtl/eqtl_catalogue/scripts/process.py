#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy, Jeremy Schwartzentruber
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
import argparse
from time import time
import pandas
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import scipy.stats as st

def main():

    # Args
    args = parse_args()
    args.min_mac = 5
    print(args)

    # # File args (test)
    # args = ArgsPlaceholder()
    # args.study_id = 'Naranbhai_2015'
    # args.in_nominal = '../example_data/Naranbhai_2015/*/*.nominal.sorted.txt.gz'
    # args.in_varinfo = '../example_data/Naranbhai_2015/*/*.variant_information.txt.gz'
    # args.in_gene_meta = '../example_data/*_phenotype_metadata.tsv.gz'
    # args.in_biofeatures_map = '../../../../genetics-backend/biofeatureLUT/biofeature_lut_190208.json'
    # args.out_parquet = '../output/Naranbhai_2015.parquet'

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    # Load data and variant table
    data = (
        load_nominal_data(args.in_nominal)
            .withColumn('n_total', (col('an') / 2).cast('int'))
            .withColumn('mac', least(col('ac'), col('an') - col('ac')))
            .withColumnRenamed('maf', 'eaf')
            .withColumnRenamed('r2', 'info')
            .drop('varid', 'type', 'ac', 'an', 'maf')
    )

    # Filter low quality variants
    data = data.filter(col('mac') >= args.min_mac)

    # Determine the number of variants tested per gene
    num_tests = (
        data
        .groupby('phenotype_id')
        .agg(count(col('pval')).alias('num_tests'))
    )

    # Merge num_tests back onto nominal data
    data = data.join(num_tests, on='phenotype_id')
          
    # In newest eQTL catalogue, gene ID is already in the nominal p values file
    # so no need to use gene metadata here
    #gene_meta = load_gene_metadata(args.in_gene_meta)
    #merged = gene_meta.join(data, on='phenotype_id', how='inner')

    # TO DO: In future may need to include quant_method (ge, exon, etc) as a column
    # and partition dataset based on that when writing. Currently we only use "ge"
    # quantification method data from eQTL catalogue.

    # bio_feature represents the tissue/condition combination, which is the qtl_group heading
    # in eQTL catalogue
    merged = data.withColumn('bio_feature', lit(args.qtl_group))

    # Additional columns to match gwas sumstat files
    merged = (
        merged.withColumn('study_id', lit(args.study_id))
              .withColumn('type', lit('eqtl'))
              .withColumn('n_cases', lit(None).cast(IntegerType()))
              .withColumn('mac_cases', lit(None).cast(IntegerType()))
              .withColumn('is_cc', lit(False))
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
    merged = merged.select(col_order)

    # Repartition and sort
    merged = (
        merged.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Write output
    (
        merged
        .write
        .partitionBy('chrom')
        .parquet(
            args.out_parquet,
            mode='overwrite',
            compression='snappy'
        )
    )

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0


def load_gene_metadata(pattern):
    df = (
        spark.read.csv(pattern,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=True) )

    # Only keep IDs
    df = (
        df.select('phenotype_id', 'gene_id')
          .distinct()
    )

    return df

def load_nominal_data(pattern):
    ''' Loads QTLtools nominal results file to spark df
    '''
    # import_schema = (
    #     StructType()
    #     .add('phenotype_id', StringType())
    #     .add('pheno_chrom', StringType())
    #     .add('pheno_start', IntegerType())
    #     .add('pheno_end', IntegerType())
    #     .add('pheno_strand', StringType())
    #     .add('num_tests', IntegerType())
    #     .add('tss_dist', IntegerType())
    #     .add('var_id', StringType())
    #     .add('chrom', StringType())
    #     .add('pos', IntegerType())
    #     .add('var_null', IntegerType())
    #     .add('pval', DoubleType())
    #     .add('beta', DoubleType())
    #     .add('is_sentinal', IntegerType())
    # )
    # Specify schema to make reading in more efficient
    import_schema = (
        StructType()
        .add('molecular_trait_id', StringType())
        .add('chromosome', StringType())
        .add('position', IntegerType())
        .add('ref', StringType())
        .add('alt', StringType())
        .add('variant', StringType())
        .add('ma_samples', IntegerType())
        .add('maf', DoubleType())
        .add('pvalue', DoubleType())
        .add('beta', DoubleType())
        .add('se', DoubleType())
        .add('type', StringType())
        .add('ac', IntegerType())
        .add('an', IntegerType())
        .add('r2', DoubleType())
        .add('molecular_trait_object_id', StringType())
        .add('gene_id', StringType())
        .add('median_tpm', DoubleType())
        .add('rsid', StringType())
    )
    df = (
        spark.read.csv(pattern,
                       sep='\t',
                       schema=import_schema,
                       enforceSchema=True, # So it will check schema against file header
                       header=False)
    )
    df = (
        df.withColumnRenamed('molecular_trait_id', 'phenotype_id')
          .withColumnRenamed('chromosome', 'chrom')
          .withColumnRenamed('position', 'pos')
          .withColumnRenamed('pvalue', 'pval')
    )

    # Remove fields we don't use:
    # 'variant', 'ma_samples', 'type', 'r2', 'molecular_trait_object_id', 'median_tpm', 'rsid'
    df = (
        df.select(['phenotype_id', 'gene_id', 'chrom', 'pos', 'ref', 'alt', 'pval', 'beta', 'se', 'maf', 'ac', 'an'])
    )

    df = (
        df.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    return df


class ArgsPlaceholder():
    pass

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--study_id', metavar="<string>", help=('Study ID to add as column'), type=str, required=True)
    parser.add_argument('--qtl_group', metavar="<string>", help=('QTL tissue/condition to add as bio_feature column'), type=str, required=True)
    parser.add_argument('--quant_method', metavar="<string>", help=('Quantification method - not currently used'), type=str, required=True)
    parser.add_argument('--in_nominal', metavar="<file>", help=('Input sum stats'), type=str, required=True)
    parser.add_argument('--in_gene_meta', metavar="<file>", help=("Input gene meta-data"), type=str, required=True)
    parser.add_argument('--in_biofeatures_map', metavar="<file>", help=("Input biofeature to ontology map"), type=str, required=True)
    parser.add_argument('--out_parquet', metavar="<file>", help=("Output parquet path"), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
