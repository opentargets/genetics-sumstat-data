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
import scipy.stats as st

def main():

    # Args
    min_mac = 5

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    start_time = time()

    for in_study in ['Alasoo_2018', 'BLUEPRINT', 'CEDAR', 'Fairfax_2012',
                     'Fairfax_2014', 'GENCORD', 'GEUVADIS', 'HipSci',
                     'Naranbhai_2015', 'Nedelec_2016', 'Quach_2016',
                     'Schwartzentruber_2018', 'TwinsUK', 'van_de_Bunt_2015']:

        # File args GCS
        in_nominal = 'gs://genetics-portal-raw/eqtl_db_v1/raw/{0}/*/*.nominal.sorted.txt.gz'.format(in_study)
        in_varinfo = 'gs://genetics-portal-raw/eqtl_db_v1/raw/{0}/*/*.variant_information.txt.gz'.format(in_study)
        in_gene_meta = 'gs://genetics-portal-raw/eqtl_db_v1/raw/*_gene_metadata.txt'
        in_biofeatures_map = 'gs://genetics-portal-data/lut/biofeature_lut_190208.json'
        out_parquet = 'gs://genetics-portal-sumstats2/molecular_qtl/{0}.parquet'.format(in_study.upper())

        # # File args local
        # in_nominal = 'example_data/{0}/*/*.nominal.sorted.txt.gz'.format(in_study)
        # in_varinfo = 'example_data/{0}/*/*.variant_information.txt.gz'.format(in_study)
        # in_gene_meta = 'example_data/*_gene_metadata.txt'
        # in_biofeatures_map = '../../../genetics-backend/biofeatureLUT/biofeature_lut_190208.json'
        # out_parquet = 'output/{0}.parquet'.format(in_study.upper())

        # Load data and variant table
        data = load_nominal_data(in_nominal)
        varinfo = load_variant_info(in_varinfo)
        meta = load_gene_metadata(in_gene_meta)

        # Filter low quality variants
        varinfo = varinfo.filter(col('MAC') >= min_mac)

        # Merge
        merged = meta.join(data, on='phenotype_id', how='inner')
        merged = merged.join(varinfo, on=['bio_feature_str', 'chrom', 'pos', 'ref', 'alt'])

        # Map bio_feature_str to bio_feature
        bf_map_dict = spark.sparkContext.broadcast(
            load_biofeatures_map(in_biofeatures_map) )
        bf_mapper = udf(lambda key: bf_map_dict.value[key])
        merged = (
            merged.withColumn('bio_feature', bf_mapper(col('bio_feature_str')))
                  .drop('bio_feature_str')
        )

        # Add study id
        merged = merged.withColumn('study_id', lit(in_study.upper()))

        # Re-order columns
        col_order = [
            'study_id',
            'bio_feature',
            'phenotype_id',
            'quant_id',
            'group_id',
            'gene_id',
            'chrom',
            'pos',
            'ref',
            'alt',
            'tss_dist',
            'beta',
            'se',
            'pval',
            'N',
            'EAF',
            'MAC',
            'num_tests',
            'is_sentinal',
            'info'
        ]
        merged = merged.select(col_order)

        # Repartition
        merged = merged.repartitionByRange('chrom', 'pos', 'ref', 'alt')

        # Write output
        (
            merged.write.parquet(
                  out_parquet,
                  mode='overwrite',
                  compression='snappy',
                  partitionBy='bio_feature'
          )
        )

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

def load_biofeatures_map(inf):
    ''' Loads file containing mapping for bio_feature_str to code
    Returns:
        python dictionary
    '''

    d = dict(
        spark.read.json(inf)
             .select('biofeature_string', 'biofeature_code')
             .toPandas()
             .values.tolist()
    )

    return d

def load_gene_metadata(pattern):
    ''' Loads the gene meta-data
    '''
    df = (
        spark.read.csv(pattern,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=True) )

    # Only keep IDs
    df = (
        df.select('phenotype_id', 'quant_id', 'group_id', 'gene_id')
          .distinct()
    )

    return df

def load_variant_info(pattern):
    ''' Loads QTLtools variant info file to spark df
    '''
    df = (
        spark.read.csv(pattern,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=False) )

    # Add column names
    cols = ['chrom', 'pos', 'varid', 'ref', 'alt', 'type', 'AC', 'AN', 'MAF',
            'info']
    df = df.toDF(*cols)

    # Calc sample size, EAF, MAC - then drop unneeded
    df = (
        df.withColumn('chrom', col('chrom').cast('string'))
          .withColumn('N', (col('AN') / 2).cast('int'))
          .withColumn('EAF', col('AC') / col('AN'))
          .withColumn('MAC', least(col('AC'), col('AN') - col('AC')))
          .drop('varid', 'type', 'AC', 'AN', 'MAF')
    )

    # Extract bio_feature
    df = df.withColumn('bio_feature_str', get_biofeature_udf(input_file_name()))

    # Repartition
    df = df.repartitionByRange('chrom', 'pos')

    return df


def load_nominal_data(pattern):
    ''' Loads QTLtools nominal results file to spark df
    '''
    df = (
        spark.read.csv(pattern,
                       sep='\t',
                       inferSchema=True,
                       enforceSchema=True,
                       header=False) )
    # Add column names
    cols = ['phenotype_id', 'pheno_chrom', 'pheno_start', 'pheno_end',
            'pheno_strand', 'num_tests', 'tss_dist', 'var_id',
            'chrom', 'pos', 'var_null', 'pval', 'beta', 'is_sentinal']
    df = df.toDF(*cols)

    # Split alleles
    parts = split(df.var_id, '_')
    df = (
        df.withColumn('ref', parts.getItem(2))
          .withColumn('alt', parts.getItem(3))
    )

    # Calculate standard errors
    df = (
        df.withColumn('z_abs', abs(ppf_udf(col('pval'))))
          .withColumn('se', col('beta') / col('z_abs'))
          .drop('z_abs')
    )

    # Add bio_feature
    df = df.withColumn('bio_feature_str', get_biofeature_udf(input_file_name()))

    # Clean fields
    df = (
        df.drop('var_null', 'pheno_strand', 'pheno_chrom', 'pheno_start',
                'pheno_end', 'var_id')
          .withColumn('chrom', df.chrom.cast('string'))
          .withColumn('is_sentinal', df.is_sentinal.cast('boolean'))
          .select(['phenotype_id', 'bio_feature_str', 'chrom', 'pos', 'ref',
                   'alt', 'pval', 'beta', 'se', 'num_tests', 'tss_dist',
                   'is_sentinal'])
    )

    # Repartition
    df = df.repartitionByRange('chrom', 'pos')

    return df

def get_biofeature(filename):
    ''' Returns biofeature from filename
    '''
    return filename.split('/')[-1].split('.')[0]
get_biofeature_udf = udf(get_biofeature, StringType())

def ppf(pval):
    ''' Return inverse cumulative distribution function of the normal
        distribution. Needed to calculate stderr.
    '''
    return float(st.norm.ppf(pval / 2))
ppf_udf = udf(ppf, DoubleType())

if __name__ == '__main__':

    main()
