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
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from time import time

def main():

    # File args GCS
    in_file_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw_unzipped/*.tsv'
    out_path = 'gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/output/saige_nov2017_sumstats_temp'

    # File args local
    # in_file_pattern = 'example_data/*.tsv'
    # out_path = 'example_out/saige_sumstats_temp'

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    start_time = time()

    # Load
    df = (
        spark.read.csv(in_file_pattern,
                        sep='\t',
                        inferSchema=True,
                        enforceSchema=True,
                        header=True)
             .withColumn('phenotype', build_pheno_udf(input_file_name()))
    )

    # Filter MAC >= 3, should already be done
    df = df.filter(df.ac >= 3)

    # Calculate OR
    df = (
        df.withColumn('OR', exp(col('beta')))
          .withColumn('OR_lowerCI', exp(col('beta') - 1.96 * col('sebeta')))
          .withColumn('OR_upperCI', exp(col('beta') + 1.96 * col('sebeta')))
    )

    # Rename columns
    print('Renaming...')
    df = (
        df.withColumnRenamed('snpid', 'variant_id')
          .withColumnRenamed('pval', 'p-value')
          .withColumnRenamed('chrom', 'chromosome')
          .withColumnRenamed('pos', 'base_pair_location')
          .withColumnRenamed('OR', 'odds_ratio')
          .withColumnRenamed('OR_lowerCI', 'ci_lower')
          .withColumnRenamed('OR_upperCI', 'ci_upper')
          .withColumnRenamed('beta', 'beta')
          .withColumnRenamed('sebeta', 'standard_error')
          .withColumnRenamed('ref', 'other_allele')
          .withColumnRenamed('alt', 'effect_allele')
          .withColumnRenamed('af', 'effect_allele_frequency')
    )

    # Write data
    print('Writing...')
    (
        df.write
          .partitionBy('phenotype')
          .csv(out_path,
               sep='\t',
               compression='gzip',
               nullValue='NA',
               mode='overwrite',
               header=True)
    )

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

def build_pheno(filename):
    ''' Returns phenotype from filename
    '''
    return filename.split('/')[-1].split('_SAIGE')[0]

build_pheno_udf = udf(build_pheno)

if __name__ == '__main__':

    main()
