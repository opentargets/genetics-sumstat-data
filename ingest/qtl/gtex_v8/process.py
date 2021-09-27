#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import sys
import os
import argparse
from datetime import datetime
import pandas
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import pprint
import logging
import io

def main():

    # Args
    args = parse_args()

    # # File args (local)
    # args = ArgsPlaceholder()
    # args.study_id = 'GTEx-sQTL'
    # args.qtl_group = 'Adipose_Subcutaneous'
    # args.in_nominal = 'example_data/Adipose_Subcutaneous.v8.EUR.sqtl_allpairs.chr21.parquet'
    # args.in_varindex = 'example_data/variant-annotation-sitelist-chr21.parquet'
    # args.out_parquet = 'example_data/output_GTEx-sQTL_Adipose_Subcutaneous.parquet'

    args.min_mac = 5

    #logger = make_logger(args.out_log)
    logString = io.StringIO()
    logger = make_logger(logString)
    log(logger, 'Starting sumstat ingest')
    log(logger, 'Args: \n' + pprint.pformat(vars(args), indent=2))
    
    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        #.config("parquet.summary.metadata.level", "ALL")
        .config("parquet.summary.metadata.level", "NONE")
        .getOrCreate()
    )
    log(logger, 'Spark version: ' + spark.version)
    start_time = datetime.now()

    # Load varindex
    varindex = (
        spark.read.parquet(args.in_varindex)
        .select(
            'chrom_b38',
            'pos_b38',
            'ref',
            'alt',
            'af.gnomad_nfe'
            #'gnomad_nfe'
        )
        .withColumnRenamed('chrom_b38', 'chrom')
        .withColumnRenamed('pos_b38', 'pos')
        .withColumnRenamed('af.gnomad_nfe', 'gnomad_nfe')
    )

    # Load sumstats
    # We load from the GTEx parquet files, so we don't need to specify a schema
    # but we leave it here for clarity of what columns we expect at minimum.
    import_schema = (
        StructType()
        .add('phenotype_id', StringType())
        .add('variant_id', StringType())
        .add('ma_count', IntegerType())
        .add('maf', DoubleType())
        .add('pval_nominal', DoubleType())
        .add('slope', DoubleType())
        .add('slope_se', DoubleType())
        )
    sumstats = (
        # args.in_nominal may have wildcard to load all separate chr* files simultaneously
        spark.read.parquet(args.in_nominal)
        .filter(col('ma_count') >= args.min_mac)
    )
    sumstats = sumstats.persist()
    log(logger, '{}: Sumstat row count'.format(sumstats.count()))

    #
    # Add sample size, tissue code, and variants to sumstat  -----------------
    #

    # Extract variant parts
    sumstats = (
        sumstats
        .withColumn('varsplit', split('variant_id', '_'))
        .withColumn('chromName', col('varsplit').getItem(0))
        .withColumn('chrom', split('chromName', 'chr').getItem(1))
        .withColumn('pos', col('varsplit').getItem(1).cast('int'))
        .withColumn('ref', col('varsplit').getItem(2))
        .withColumn('alt', col('varsplit').getItem(3))
        .drop('varsplit', 'variant_id')
    )

    # Extract gene_id and expression 'feature'
    # The phenotype_id for a splicing junction is like: chr1:15038:15796:clu_50677:ENSG00000227232.5
    # We use a regex ':(?!.*:)' to match the last : in the string, to get everything up until the gene ID.
    # This uses a negative lookahead. See it working on regexr: https://regexr.com/63fqh
    sumstats = (
        sumstats
        .withColumn('gene_id', split('phenotype_id', ':').getItem(4))
        .withColumn('splicing_cluster', split('phenotype_id', ':').getItem(3))
        .withColumn('expr_feature', split('phenotype_id', ':(?!.*:)').getItem(0))
    )

    # Add sample size
    sumstats = (
        sumstats
        .withColumn('n_total', ((col('ma_count') / col('maf')) / 2).cast('int') )
    )
    sumstats = sumstats.persist()

    #
    # Figure out which allele is the effect allele for each variant ------------
    #

    # Perform left join with gnomad index
    intersection = sumstats.join(
        varindex,
        on=['chrom', 'pos', 'ref', 'alt'],
        how='left'
    )
    sumstats = sumstats.persist()

    # Only keep those in gnomad
    intersection = intersection.filter(
        col('gnomad_nfe').isNotNull()
    )
    log(logger, '{}: row count after gnomad intersection'.format(intersection.count()))
    #intersection.show(n=3)

    # If gnomad_nfe <= 0.5 assume that alt is the minor allele
    df = (
        intersection
        .withColumn('eaf',
            when(col('gnomad_nfe') <= 0.5,
            col('maf')).otherwise(1-col('maf')))
    )
    df = df.persist()

    #
    # Calculate the number of tests and min pval per splice junction ----------
    # We could count the number of tests per cluster (across all junctions)
    # but this would be too strict, since junction results are correlated.
    #
    min_junction_pvals = (
        df
        .groupby('expr_feature', 'gene_id')
        .agg(count(col('pval_nominal')).alias('num_tests'),
             min(col('pval_nominal')).alias('min_junction_pval'))
    )
    log(logger, '{}: number of expression features (cluster junctions) total'.format(min_junction_pvals.count()))
    #min_junction_pvals.show(n=3)

    # Merge num_tests and min_junction_pval back onto the sumstats
    df = df.join(min_junction_pvals, on=['expr_feature', 'gene_id'])

    # Join the minimum pval per cluster to the main df
    min_cluster_pvals = (
        df
        .groupby('splicing_cluster', 'gene_id')
        .agg(min(col('pval_nominal')).alias('min_cluster_pval'))
    )
    df = df.persist()

    df = df.join(
        min_cluster_pvals,
        on=['splicing_cluster', 'gene_id'],
        how='left'
    )
    df = df.persist()

    # Each variant is now annotated with the min pval for its junction and for
    # its cluster. Remove all rows where the min junction pval is above the
    # min cluster pval.
    # It is possible that two junctions will have identical min_p values,
    # and so both will be kept, but this should be rare. Still, we'll have
    # to make sure that downstream steps don't break if >1 junction is kept
    # for a cluster.
    df = df.filter(col('min_junction_pval') <= col('min_cluster_pval'))
    log(logger, '{}: row count after filtering for best junction'.format(df.count()))
    df = df.persist()

    min_junction_pvals = (
        df
        .groupby('expr_feature', 'gene_id')
        .agg(count(col('pval_nominal')).alias('num_tests'))
    )
    df = df.persist()
    log(logger, '{}: number of expression features (cluster junctions) retained'.format(min_junction_pvals.count()))

    #
    # Tidy up and write -------------------------------------------------------
    #

    # Format columns
    df = (
        df
        .withColumn('type', lit('sqtl'))
        .withColumn('study_id', lit(args.study_id))
        .withColumn('phenotype_id', col('gene_id'))
        .withColumn('bio_feature', lit(args.qtl_group))
        .withColumnRenamed('slope', 'beta')
        .withColumnRenamed('slope_se', 'se')
        .withColumnRenamed('pval_nominal', 'pval')
        .withColumn('n_cases', lit(None).cast('int'))
        .withColumnRenamed('ma_count', 'mac')
        .withColumn('mac_cases', lit(None).cast('int'))
        .withColumn('info', lit(None).cast('float'))
        .withColumn('is_cc', lit(False))
    )

    # Order and select columns
    df = (
        df.select(
            'type',
            'study_id',
            'phenotype_id',
            'bio_feature',
            'gene_id',
            'expr_feature',
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
        )
    )

    # Drop NA rows
    required_cols = ['type', 'study_id', 'phenotype_id', 'bio_feature', 'expr_feature',
                    'gene_id', 'chrom', 'pos', 'ref', 'alt', 'beta', 'se', 'pval']
    df = df.dropna(subset=required_cols)
    df = df.persist()
    log(logger, '{}: final row count (after dropping NAs)'.format(df.count()))

    # Repartition and sort
    df = (
        df.repartitionByRange('chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Write output
    (
        df
        .write
        .partitionBy('chrom')
        .parquet(
            args.out_parquet,
            mode='append',
            compression='snappy'
        )
    )

    df.unpersist()

    log(logger, 'Time taken: {}'.format(datetime.now() - start_time))
    saveSparkTextFile(logString.getvalue(), args.out_log)

    return 0

# This is a kind of hack to save text to a google cloud storage file from Spark.
# It seems that you can't write to cloud storage just from python code, and in
# Spark you can only save an RDD or Dataframe.
def saveSparkTextFile(text_str, filepath):
    (spark.createDataFrame(
        [([text_str])])
        .coalesce(1)
        .write
        .format("text")
        .option("header", "false")
        .mode("overwrite")
        .save(filepath) )


def log(logger, msg):
    logger.info(msg)

# We can't write directly to a GCS file from a Dataproc cluster, and so we save
# up the log text in a string stream object and later write it as a Dataframe.
def make_logger(str_stream):
    ''' Creates a logging handle.
    '''
    # Basic setup
    logging.basicConfig(
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=None)
    # Create formatter
    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    rootLogger = logging.getLogger(__name__)
    # Add file logging
    fileHandler = logging.StreamHandler(str_stream)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    # Add stdout logging
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
     # Prevent logging from propagating to the root logger
    rootLogger.propagate = 0
    
    return rootLogger


class ArgsPlaceholder():
    pass

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--study_id', metavar="<string>", help=('Study ID to add as column'), type=str, required=True)
    parser.add_argument('--qtl_group', metavar="<string>", help=('QTL tissue/condition to add as bio_feature column'), type=str, required=True)
    parser.add_argument('--in_nominal', metavar="<file>", help=('Input sum stats'), type=str, required=True)
    parser.add_argument('--in_varindex', metavar="<file>", help=('Variant index'), type=str, required=True)
    parser.add_argument('--out_parquet', metavar="<file>", help=("Output parquet path"), type=str, required=True)
    parser.add_argument('--out_log', metavar="<file>", help=("Output log file path"), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
