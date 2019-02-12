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

def main():

    # Args
    min_mac = 10

    # File args GCS

    # File args local
    # in_sumstat = 'example_data/26192919-GCST003044-EFO_0000384.h.tsv'
    in_sumstat = 'example_data/custom.tsv' # DEBUG
    in_af = 'example_data/variant-annotation_af-only_chrom10.parquet'
    out_parquet = 'output/26192919-GCST003044-EFO_0000384.parquet'

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    start_time = time()

    # Load data
    data = load_sumstats(in_sumstat)

    #
    # Fill in effect allele frequency using gnomad NFE frequency ---------------
    #

    # If there are any nulls in eaf, get allele freq from reference
    if data.filter(col('eaf').isNull()).count() > 0:

        # Load gnomad allele frequencies
        afs = (
            spark.read.parquet(in_af)
                 .select('chrom_b38', 'pos_b38', 'ref', 'alt', 'af.gnomad_nfe')
                 .withColumnRenamed('chrom_b38', 'chrom')
                 .withColumnRenamed('pos_b38', 'pos')
        )

        # Join
        data = data.join(afs, on=['chrom', 'pos', 'ref', 'alt'], how='left')

        # Make fill in blanks on the EAF column using gnomad AF
        data = (
            data.withColumn('eaf', when(col('eaf').isNull(), col('gnomad_nfe')).otherwise(col('eaf')))
                .drop('gnomad_nfe')
        )

    #
    # Fill in effect allele frequency using gnomad NFE frequency ---------------
    #

    # Add sample size, case numbers

    # Calculate and filter based on MAC







    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

def load_sumstats(inf):
    ''' Load a harmonised GWAS Catalog file
    '''
    # Read
    df = ( spark.read.csv(inf,
                          sep='\t',
                          inferSchema=True,
                          enforceSchema=True,
                          header=True,
                          nullValue='NA') )

    # If "low_confidence_variant" exists, filter based on it
    # TODO when we have Neale V2 data

    # Specify new names and types
    column_d = OrderedDict([
        ('chromosome', ('chrom', StringType())),
        ('base_pair_location', ('pos', IntegerType())),
        ('hm_other_allele', ('ref', StringType())),
        ('hm_effect_allele', ('alt', StringType())),
        ('p_value', ('pval', DoubleType())),
        ('hm_beta', ('beta', DoubleType())),
        ('standard_error', ('se', DoubleType())),
        ('hm_odds_ratio', ('oddsr', DoubleType())),
        ('hm_ci_lower', ('oddsr_lower', DoubleType())),
        ('hm_effect_allele_frequency', ('eaf', DoubleType()))
    ])

    # Add missing columns as null
    for column in column_d.keys():
        if column not in df.columns:
            df = df.withColumn(column, lit(None).cast(column_d[column][1]))

    # Reorder columns
    df = df.select(*list(column_d.keys()))

    # Change type and name of all columns
    for column in column_d.keys():
        df = ( df.withColumn(column, col(column).cast(column_d[column][1]))
                 .withColumnRenamed(column, column_d[column][0]) )

    # Replace beta/se with logOR/logORse if oddsr and oddsr_lower not null
    perc_97th = 1.95996398454005423552
    to_do = (df.oddsr.isNotNull() & df.oddsr_lower.isNotNull())
    df = (
        df.withColumn('beta', when(to_do, log(df.oddsr)).otherwise(df.beta))
          .withColumn('se', when(to_do, (log(df.oddsr) - log(df.oddsr_lower))/perc_97th ).otherwise(df.se))
          .drop('oddsr', 'oddsr_lower')
    )

    # Impute standard error if missing
    to_do = (df.beta.isNotNull() & df.pval.isNotNull() & df.se.isNull())
    df = (
        df.withColumn('z_abs', abs(ppf_udf(col('pval'))))
          .withColumn('se', when(to_do, abs(col('beta')) / col('z_abs')).otherwise(col('se')))
          .drop('z_abs')
    )

    # Drop NAs, eaf null is ok as this will be inferred from a reference
    df = df.dropna(subset=['chrom', 'pos', 'ref', 'alt', 'pval', 'beta', 'se'])

    # Repartition
    df = df.repartitionByRange('chrom', 'pos')

    return df

def ppf(pval):
    ''' Return inverse cumulative distribution function of the normal
        distribution. Needed to calculate stderr.
    '''
    return float(st.norm.ppf(pval / 2))
ppf_udf = udf(ppf, DoubleType())

if __name__ == '__main__':

    main()
