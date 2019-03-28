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

def main():

    # Args
    min_mac = 5
    pheno_var = 1
    study_id = 'eQTLGen'
    bio_feature = 'UBERON_0000178'
    data_type = 'eqtl'

    # File args (local)
    in_sumstats = 'example_data/cis-eQTLs_full_20180905.head.txt'
    in_varindex = 'example_data/variant-annotation.sitelist.tsv'
    in_freqs = 'example_data/2018-07-18_SNP_AF_for_AlleleB_combined_allele_counts_and_MAF_pos_added.txt'
    out_parquet = 'output/eQTLGen'
    
    # File args (server)
    # TODO

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    #
    # Load --------------------------------------------------------------------
    #

    # Load sumstats
    import_schema = (
        StructType()
        .add('pval', DoubleType())
        .add('rsid', StringType())
        .add('chrom', StringType())
        .add('pos', IntegerType())
        .add('z', DoubleType())
        .add('effect_allele', StringType())
        .add('other_allele', StringType())
        .add('gene_id', StringType())
        .add('gene_name', StringType())
        .add('gene_chrom', StringType())
        .add('gene_pos', IntegerType())
        .add('n_cohorts', IntegerType())
        .add('n_total', IntegerType())
        )
    sumstats = (
        spark.read.csv(
            path=in_sumstats,
            sep='\t',
            schema=import_schema,
            enforceSchema=True,
            header=True,
            comment='#')
        .drop('rsid', 'gene_name', 'gene_chrom', 'gene_pos', 'n_cohorts')
    )

    # print(sumstats.select("pval").rdd.histogram(10))
    # sumstats.show()
    # sys.exit()

    # Load freqs
    import_schema = (
        StructType()
        .add('rsid', StringType())
        .add('chrom', StringType())
        .add('pos', IntegerType())
        .add('effect_allele', StringType())
        .add('other_allele', StringType())
        .add('a_count', IntegerType())
        .add('ab_count', IntegerType())
        .add('b_count', IntegerType())
        .add('b_freq', DoubleType())
        )
    freqs = (
        spark.read.csv(
            path=in_freqs,
            sep='\t',
            schema=import_schema,
            enforceSchema=True,
            header=True,
            comment='#')
        .withColumn('eaf', 1 - col('b_freq'))
        .drop('rsid', 'a_count', 'ab_count', 'b_count', 'b_freq')
    )

    # Load varindex
    print('WARNING: using test variant index')
    sys.exit('EXITING to make sure I dont forget to change this in production')
    import_schema = (
        StructType()
        .add('locus', StringType())
        .add('alleles', StringType())
        .add('chrom', StringType())
        .add('pos', IntegerType())
        .add('chrom_b38', StringType())
        .add('pos_b38', IntegerType())
        .add('ref', StringType())
        .add('alt', StringType())
        .add('rsid', StringType())
    )
    varindex = (
        spark.read.csv(
            path=in_varindex,
            sep='\t',
            schema=import_schema,
            enforceSchema=True,
            header=True,
            comment='#')
        .drop('locus', 'alleles', 'rsid')
    )
    # # Load varindex
    # varindex = (
    #     spark.read.parquet(in_varindex)
    #     .select(
    #         'chrom_b37',
    #         'pos_b37',
    #         'chrom_b38',
    #         'pos_b38',
    #         'ref',
    #         'alt',
    #         'af.gnomad_nfe'
    #     )
    #     .withColumnRenamed('chrom_b37', 'chrom')
    #     .withColumnRenamed('pos_b37', 'pos')
    # )

    #
    # Merge allele freqs ------------------------------------------------------
    #

    sumstats = sumstats.join(
        freqs,
        on=['chrom', 'pos', 'effect_allele', 'other_allele']
    )

    #
    # Harmonised other and effect alleles to be ref and alt, respectively -----
    #

    # Left merge sumstats with gnomad
    merged = (
        sumstats.join(varindex,
        (
            (varindex.chrom == sumstats.chrom) &
            (varindex.pos == sumstats.pos) &
            ( 
                ((varindex.ref == sumstats.other_allele) & (varindex.alt == sumstats.effect_allele)) |
                ((varindex.ref == sumstats.effect_allele) & (varindex.alt == sumstats.other_allele))
            )
        ))
    )

    # If effect_allele == ref, flip z-score and eaf
    merged = (
        merged
        .withColumn('z', when(col('effect_allele') == col('ref'), -1 * col('z')).otherwise(col('z')))
        .withColumn('eaf', when(col('effect_allele') == col('ref'), 1 - col('eaf')).otherwise(col('eaf')))
        .drop('effect_allele', 'other_allele')
    )

    #
    # Estimate beta and SE ----------------------------------------------------
    #

    # Equation from eQTLGen paper
    # beta =     z / (√(2p(1-p)(n+z^2))
    # SE(beta) = 1 / (√(2p(1-p)(n+z^2))
    merged = (
        merged
        .withColumn('beta', col('z')/((2*col('eaf')*(1-col('eaf'))*(col('n_total')+col('z')**2))**0.5))
        .withColumn('se', 1/((2*col('eaf')*(1-col('eaf'))*(col('n_total')+col('z')**2))**0.5))
        .drop('z')
    )
    
    #
    # Calc number of tests per phenotype_id -----------------------------------
    #

    merged = (
        merged
        .withColumn('phenotype_id', col('gene_id'))
        .persist()
    )

    # Count number of tests
    num_tests = (
        merged
        .groupby('phenotype_id')
        .agg(count(col('pval')).alias('num_tests'))
    )

    # Merge result back onto merged
    merged = merged.join(num_tests, on='phenotype_id')

    #
    # Tidy up and write -------------------------------------------------------
    #

    df = merged

    # Format columns
    df = (
        df
        # Use build 38 chrom and positions
        .drop('chrom', 'pos', 'effect_allele', 'other_allele')
        .withColumnRenamed('chrom_b38', 'chrom')
        .withColumnRenamed('pos_b38', 'pos')
        # Add new columns
        .withColumn('study_id', lit(study_id))
        .withColumn('type', lit(data_type))
        .withColumn('bio_feature', lit(bio_feature))
        .withColumn('n_cases', lit(None).cast('int'))
        .withColumn('is_cc', lit(False))
        .withColumn('maf', when(col('eaf') > 0.5, 1 - col('eaf')).otherwise(col('eaf')))
        .withColumn('mac', col('n_total') * 2 * col('maf'))
        .withColumn('mac_cases', lit(None).cast('int'))
        .withColumn('info', lit(None).cast('double'))
        # Filter based on mac
        .filter(col('mac') >= min_mac)
    )

    # Order and select columns
    df = (
        df.select(
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
            'num_tests',
            'info',
            'is_cc'
        )
    )

    # Drop NA rows
    required_cols = ['type', 'study_id', 'phenotype_id', 'bio_feature',
                     'gene_id', 'chrom', 'pos', 'ref', 'alt', 'beta', 'se', 'pval']
    df = df.dropna(subset=required_cols)

    # Repartition and sort
    df = (
        df.repartitionByRange('chrom', 'pos')
        .orderBy('chrom', 'pos', 'ref', 'alt')
    )

    # Write output
    (
        df
        .write
        .partitionBy('bio_feature', 'chrom')
        .parquet(
            out_parquet,
            mode='overwrite',
            compression='snappy'
        )
    )

    return 0

if __name__ == '__main__':

    main()
