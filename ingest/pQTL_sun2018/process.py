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
    study_id = 'SUN2018'
    bio_feature = 'UBERON_0001969'
    data_type = 'pqtl'
    sample_size = 3301
    cis_dist = 1e6

    # File args (local)
    in_sumstats = 'example_data/pqtls/*/*.tsv'
    in_varindex = 'example_data/variant-annotation.sitelist.tsv'
    in_genes = 'example_data/gene_dictionary.json'
    in_manifest = 'example_data/001_SOMALOGIC_GWAS_protein_info.csv'
    in_ensembl_map = 'example_data/Sun_pQTL_uniprot_ensembl_lut.tsv'
    out_parquet = 'output/SUN2018'
    
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
        .add('variant_id', StringType())
        .add('chrom', StringType())
        .add('pos', IntegerType())
        .add('effect_allele', StringType())
        .add('other_allele', StringType())
        .add('beta', DoubleType())
        .add('se', DoubleType())
        .add('log10_pval', DoubleType())
        )
    sumstats = (
        spark.read.csv(
            path=in_sumstats,
            sep='\t',
            schema=import_schema,
            enforceSchema=True,
            header=True,
            comment='#')
        .drop('variant_id')
        .withColumn('effect_allele', upper(col('effect_allele')))
        .withColumn('other_allele', upper(col('other_allele')))
        .withColumn('pval', 10 ** col('log10_pval'))
        .drop('log10_pval')
    )

    # Load varindex
    print('WARNING: using test variant index, including random eaf')
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
        .withColumn('eaf', rand())
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

    # Load manifest
    manifest = (
        spark.read.csv(in_manifest,
                              sep=',',
                              header=True)
        .drop('TargetFullName', 'Target')
        .withColumnRenamed('SOMAMER_ID', 'phenotype_id')
    )

    # Load ensembl id map
    ensembl_map = (
        spark.read.csv(in_ensembl_map,
                       sep='\t',
                       header=True)
        .withColumnRenamed('Uniprot', 'UniProt')
        .withColumnRenamed('Ensembl', 'gene_id')
        .filter(col('gene_id').startswith('ENSG'))
    )
    
    # Load gene dictionary
    gene_dict = (
        spark.read.json(in_genes)
        .select('gene_id', 'chr', 'tss')
        .withColumnRenamed('chr', 'chrom')
        
    )

    #
    # Filter sumstats to keep cis variants only -------------------------------
    #

    # Get phenotype_id from sumstat filename
    get_phenotype = udf(lambda filename: filename.split('/')[-1].split('_')[0])
    sumstats = (
        sumstats.withColumn('phenotype_id', get_phenotype(input_file_name()))
    )

    # Merge manfiest, gene_map and gene_dict
    manifest = (
        manifest
        .join(ensembl_map, on='UniProt')
        .join(gene_dict, on='gene_id')
    )

    # Merge manifest to sumstats
    sumstats = sumstats.join(
        manifest,
        on=['phenotype_id', 'chrom']
    )

    # Filter to only keep variants within cis_dist of gene tss
    sumstats = sumstats.filter(
        abs(col('pos') - col('tss')) <= cis_dist
    )

    # Drop uneeded cols
    sumstats = (
        sumstats
        .drop('UniProt', 'tss')
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

    # If effect_allele == ref, flip beta
    merged = (
        merged
        .withColumn('beta', when(col('effect_allele') == col('ref'), -1 * col('beta')).otherwise(col('beta')))
    )

    #
    # Calc number of tests per phenotype_id -----------------------------------
    #

    merged = merged.persist()

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
        .withColumn('n_total', lit(sample_size))
        .withColumn('n_cases', lit(None).cast('int'))
        .withColumn('is_cc', lit(False))
        .withColumn('mac', col('n_total') * 2 * col('eaf'))
        .withColumn('mac_cases', lit(None).cast('int'))
        .withColumn('info', lit(None).cast('double'))
        # Replace . with _ in phenotype ID
        .withColumn('phenotype_id', regexp_replace(col('phenotype_id'), '\.', '_'))
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
