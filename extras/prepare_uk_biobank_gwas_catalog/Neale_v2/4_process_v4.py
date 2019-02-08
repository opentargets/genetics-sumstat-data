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
import os
import gzip
import pyspark.sql
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from time import time
from glob import glob
import subprocess as sp

def main():

    # File args GCS
    # in_file_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw_unzipped/10000*_raw.*.tsv'
    in_file_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw_unzipped/*.tsv'
    in_variant_index = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/variants.nealev2.parquet'
    in_phenotype = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/phenotypes.both_sexes.filtered.tsv'
    out_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats_temp2/{pheno}'

    # File args local
    # in_file_pattern = 'example_data/*.head.tsv'
    # # in_file_pattern = 'example_data2/1*.tsv'
    # in_variant_index = '2_make_variant_index/variants.nealev2.parquet'
    # in_phenotype = '1_stream_to_gcs/manifest/phenotypes.both_sexes.filtered.tsv'
    # out_pattern = 'example_out/neale_v2_sumstats_temp/{pheno}'

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    start_time = time()

    #
    # Load and prep datasets ------------------------------------------------
    #

    print('Loading variant table...')
    variants = ( spark.read.parquet(in_variant_index)
                      .select('chr', 'pos', 'ref', 'alt', 'rsid', 'info')
                      .sort('chr', 'pos', 'ref', 'alt')
                      .cache()
                )

    # Load phenotype info into a dict
    print('Loading phenotype table...')
    pheno_dict = (
        spark.read.csv(in_phenotype,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=True)
            .select('phenotype',
                    'description',
                    'source',
                    'n_non_missing',
                    'n_missing',
                    'n_controls',
                    'n_cases')
            .toPandas()
            .set_index('phenotype')
            .to_dict(orient='index')
    )
    pheno_dict_b = spark.sparkContext.broadcast(pheno_dict)

    # Iterate over sumstats
    for i, inf in enumerate(glob_gcs(in_file_pattern)):

        print('Processing ({0}) {1}...'.format(i + 1, inf))

        # Create output name
        pheno_name = build_pheno(inf)
        outf = out_pattern.format(pheno=pheno_name)
        if gcs_exists(outf + '/_SUCCESS'):
            print('Output exists, skipping...')
            continue

        start_time_inner = time()

        # Load sumstats
        df = (spark.read.csv(inf,
                             sep='\t',
                             inferSchema=True,
                             enforceSchema=False,
                             header=True)
                   .withColumn('phenotype', lit(pheno_name))
        )


        # Split variant ID
        print('Spliting variant ID...')
        parts = split(df.variant, ':')
        df = (
            df.withColumn('chr', parts.getItem(0))
              .withColumn('pos', parts.getItem(1).cast('long'))
              .withColumn('ref', parts.getItem(2))
              .withColumn('alt', parts.getItem(3))
        )

        #
        # Join additional columns --------------------------------------------------
        #

        # Join variants
        print('Joining variant table...')
        df = df.join(variants, on=['chr', 'pos', 'ref', 'alt'], how='inner')

        # Join phenotype info. A join doesnt work here as there is only 1 phenotype
        print('Joining phenotype table...')
        for col in ['description', 'n_non_missing', 'n_missing', 'n_controls', 'n_cases']:
            df = df.withColumn(col, lit(pheno_dict_b.value[pheno_name][col]))

        #
        # Process ------------------------------------------------------------------
        #

        # Calc logOR and logORse
        print('Calc OR...')
        case_frac = df.n_cases / (df.n_cases + df.n_controls)
        logOR = df.beta / (case_frac * (1 - case_frac))
        logORse = df.se / (case_frac * (1 - case_frac))
        df = (
               df.withColumn('logOR', logOR)
                 .withColumn('logORse', logORse)
                 .withColumn('OR', exp(logOR))
        )
        df = (
            df.withColumn('OR_lowerCI', exp(df.logOR - 1.96 * df.logORse))
              .withColumn('OR_upperCI', exp(df.logOR + 1.96 * df.logORse))
        )

        # Calculcate allele frequency
        print('Calc AF...')
        df = df.withColumn('AF', df.AC / (2 * df.n_complete_samples))

        # Rename columns
        print('Renaming...')
        df = ( df.withColumnRenamed('rsid', 'variant_id')
                 .withColumnRenamed('pval', 'p-value')
                 .withColumnRenamed('chr', 'chromosome')
                 .withColumnRenamed('pos', 'base_pair_location')
                 .withColumnRenamed('OR', 'odds_ratio')
                 .withColumnRenamed('OR_lowerCI', 'ci_lower')
                 .withColumnRenamed('OR_upperCI', 'ci_upper')
                 .withColumnRenamed('beta', 'beta')
                 .withColumnRenamed('se', 'standard_error')
                 .withColumnRenamed('ref', 'other_allele')
                 .withColumnRenamed('alt', 'effect_allele')
                 .withColumnRenamed('AF', 'effect_allele_frequency')
        )

        #
        # Output ---------------------------------------
        #

        print('Writing...')

        # Write header
        header_file = outf + '.header'
        (
            spark.sparkContext
                 .parallelize(df.columns, numSlices=1)
                 .saveAsTextFile(header_file)
        )

        # Write data
        (
            df.write.csv(outf,
                         sep='\t',
                         compression='gzip',
                         nullValue='NA',
                         mode='overwrite',
                         header=False)
        )

        # Unpersist
        df.unpersist()

        print('Finished iteration in {:.1f} secs'.format(time() - start_time_inner))

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

def build_pheno(filename):
    ''' Returns phenotype from filename
    '''
    return filename.split('/')[-1].split('.')[0]

def union_all(dfs):
    ''' Take union of all spark dataframes
    Params:
        dfs ([spk.df])
    Returns
        spk.df
    '''

    # Get list of all columns names, maintaining original order
    cols = dedup_list(
        reduce(lambda x, y: x + y,
               map(lambda z: z.columns, dfs)
    ))

    # All dfs must have the same set of columns
    for i, df in enumerate(dfs):
        for missing_col in set(cols) - set(df.columns):
            dfs[i] = dfs[i].withColumn(missing_col, lit(None))
        dfs[i] = dfs[i].select(cols)

    # Take union
    union = reduce(DataFrame.union, dfs)

    return union


def dedup_list(seq):
    ''' Remove duplicates from a list whilst maintaining order
    '''
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def glob_gcs(url):
    ''' Glob that works for google storage
    '''
    if url.startswith('gs://'):
        proc = sp.Popen('gsutil ls {}'.format(url), shell=True, stdout=sp.PIPE)
        for line in proc.stdout:
           yield line.decode("utf-8").rstrip()
    else:
        for inf in glob(url):
            yield inf

def gcs_exists(path):
    ''' Check if GCS path exists using gsutils
    '''
    if path.startswith('gs://'):
        cmd = 'gsutil ls {0}'.format(path)
        with open(os.devnull, 'w') as fnull:
            cp = sp.run(cmd, shell=True, stdout=fnull, stderr=sp.STDOUT)

        if cp.returncode == 0:
            return True
        else:
            return False
    else:
        os.path.exists(path)

if __name__ == '__main__':

    main()
