#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Requires pandas

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
import os
from time import time
import pandas
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def main():

    # Args
    min_mac = 5

    # # File args (local)
    # tissues = ['Adipose_Subcutaneous', 'Brain_Cortex', 'Liver']
    # in_gtex = 'example_data/gtex/{tissue}.allpairs.tsv'
    # in_varindex = 'example_data/varindex_part-00000-2f1d26b6-a5b6-428f-9e62-affcd1ef6971-c000.snappy.parquet'
    # in_biofeature_map = 'example_data/biofeature_lut_190208.json'
    # out_parquet = 'output/GTEX_v7_2'
    
    # File args (server)
    in_gtex = 'gs://genetics-portal-raw/eqtl_gtex_v7/allpairs_split/{tissue}.allpairs.txt.*.gz'
    in_varindex = 'gs://genetics-portal-data/variant-annotation/190129/variant-annotation.parquet'
    in_biofeature_map = 'gs://genetics-portal-data/lut/biofeature_lut_190328.json'
    out_parquet = 'gs://genetics-portal-sumstats-b38/unfiltered/molecular_trait/GTEX_v7.parquet'
    tissues = [
        'Adipose_Subcutaneous',
        'Adipose_Visceral_Omentum',
        'Adrenal_Gland',
        'Artery_Aorta',
        'Artery_Coronary',
        'Artery_Tibial',
        'Brain_Amygdala',
        'Brain_Anterior_cingulate_cortex_BA24',
        'Brain_Caudate_basal_ganglia',
        'Brain_Cerebellar_Hemisphere',
        'Brain_Cerebellum',
        'Brain_Cortex',
        'Brain_Frontal_Cortex_BA9',
        'Brain_Hippocampus',
        'Brain_Hypothalamus',
        'Brain_Nucleus_accumbens_basal_ganglia',
        'Brain_Putamen_basal_ganglia',
        'Brain_Spinal_cord_cervical_c-1',
        'Brain_Substantia_nigra',
        'Breast_Mammary_Tissue',
        'Cells_EBV-transformed_lymphocytes',
        'Cells_Transformed_fibroblasts',
        'Colon_Sigmoid',
        'Colon_Transverse',
        'Esophagus_Gastroesophageal_Junction',
        'Esophagus_Mucosa',
        'Esophagus_Muscularis',
        'Heart_Atrial_Appendage',
        'Heart_Left_Ventricle',
        'Liver',
        'Lung',
        'Minor_Salivary_Gland',
        'Muscle_Skeletal',
        'Nerve_Tibial',
        'Ovary',
        'Pancreas',
        'Pituitary',
        'Prostate',
        'Skin_Not_Sun_Exposed_Suprapubic',
        'Skin_Sun_Exposed_Lower_leg',
        'Small_Intestine_Terminal_Ileum',
        'Spleen',
        'Stomach',
        'Testis',
        'Thyroid',
        'Uterus',
        'Vagina',
        'Whole_Blood'
    ]

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .config("parquet.enable.summary-metadata", "true")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)
    start_time = time()

    # Load varindex
    varindex = (
        spark.read.parquet(in_varindex)
        .select(
            'chrom_b37',
            'pos_b37',
            'chrom_b38',
            'pos_b38',
            'ref',
            'alt',
            'af.gnomad_nfe'
        )
        .withColumnRenamed('chrom_b37', 'chrom')
        .withColumnRenamed('pos_b37', 'pos')
    )
    
    # Load biofeature map
    biofeature_map = dict(
        spark.read.json(in_biofeature_map)
        .select('biofeature_string', 'biofeature_code')
        .toPandas()
        .values.tolist()
    )

    # Process each tissue separately
    for tissue in tissues:

        #
        # Load --------------------------------------------------------------------
        #

        # Load sumstats
        import_schema = (
            StructType()
            .add('gene_id', StringType())
            .add('variant_id', StringType())
            .add('tss_distance', IntegerType())
            .add('ma_samples', IntegerType())
            .add('ma_count', IntegerType())
            .add('maf', DoubleType())
            .add('pval_nominal', DoubleType())
            .add('slope', DoubleType())
            .add('slope_se', DoubleType())
            )
        sumstats = (
            spark.read.csv(
                path=in_gtex.format(tissue=tissue),
                sep='\t',
                schema=import_schema,
                enforceSchema=True,
                header=True,
                comment='#')
            .filter(col('ma_count') >= min_mac)
        )

        #
        # Add sample size, tissue code, and variants to sumstat  -----------------
        #

        # Extract variant parts
        sumstats = (
            sumstats
            .withColumn('varsplit', split('variant_id', '_'))
            .withColumn('chrom', col('varsplit').getItem(0))
            .withColumn('pos', col('varsplit').getItem(1).cast('int'))
            .withColumn('ref', col('varsplit').getItem(2))
            .withColumn('alt', col('varsplit').getItem(3))
            .drop('varsplit', 'variant_id')
        )

        # Extract gene_id
        sumstats = (
            sumstats
            .withColumn('gene_id', split('gene_id', '\.').getItem(0))
        )

        # Add biofeature code and sample size
        biofeature_mapper = udf(lambda key: biofeature_map[key])
        sumstats = (
            sumstats
            .withColumn('bio_feature_str', get_biofeature_udf(input_file_name()))
            .withColumn('bio_feature', biofeature_mapper('bio_feature_str'))
            .withColumn('n_total', ((col('ma_count') / col('maf')) / 2).cast('int') )
        )

        #
        # Figure out which allele is the effect allele for each variant ------------
        #

        # Perform left join with gnomad index
        intersection = sumstats.join(
            varindex,
            on=['chrom', 'pos', 'ref', 'alt'],
            how='left'
        )

        # Only keep those in gnomad
        intersection = intersection.filter(
            col('gnomad_nfe').isNotNull()
        )

        # If gnomad_nfe <= 0.5 assume that alt is the minor allele
        intersection = (
            intersection
            .withColumn('eaf',
                when(col('gnomad_nfe') <= 0.5,
                col('maf')).otherwise(1-col('maf')))
        )

        #
        # Calculate the number of tests per (bio_feature, gene_id) ------------
        #

        intersection = intersection.persist()

        # Count number of tests
        num_tests = (
            intersection
            .groupby('bio_feature', 'gene_id')
            .agg(count(col('pval_nominal')).alias('num_tests'))
        )

        # Merge result back onto intersection
        intersection = intersection.join(num_tests, on=['bio_feature', 'gene_id'])

        #
        # Tidy up and write -------------------------------------------------------
        #

        df = intersection

        # Format columns
        df = (
            df
            .withColumn('type', lit('eqtl'))
            .withColumn('study_id', lit('GTEX_v7'))
            .withColumn('phenotype_id', col('gene_id'))
            .drop('chrom', 'pos')
            .withColumnRenamed('chrom_b38', 'chrom')
            .withColumnRenamed('pos_b38', 'pos')
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
        )

        # Write output
        (
            df
            .write
            .partitionBy('bio_feature', 'chrom')
            .parquet(
                out_parquet,
                mode='append',
                compression='snappy'
            )
        )

        intersection.unpersist()
        df.unpersist()
    
    return 0

def get_biofeature(filename):
    ''' Returns biofeature from filename
    '''
    return filename.split('/')[-1].split('.')[0]
get_biofeature_udf = udf(get_biofeature, StringType())

if __name__ == '__main__':

    main()
