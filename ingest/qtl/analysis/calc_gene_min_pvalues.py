#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import sys
import os
import pyspark.sql
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import functools
from gcsfs import GCSFileSystem

def main():

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        #.config("parquet.summary.metadata.level", "ALL")
        .config("parquet.summary.metadata.level", "NONE")
        .getOrCreate()
    )
    start_time = datetime.now()

    # Load all molecular trait sumstats
    # This has to be done separately, followed by unionByName as the hive
    # parititions differ across datasets due to different tissues
    # (bio_features) and chromosomes
    strip_path_mol = udf(lambda x: x.replace('file:', ''), StringType())
    mol_dfs = []
    mol_pattern = 'gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait/'
    fs = GCSFileSystem()
    # List files; remove trailing '/' and deduplicate
    paths = list(set([s.rstrip('/') for s in fs.glob(mol_pattern)]))
    #paths = ['genetics-portal-dev-sumstats/unfiltered/molecular_trait/BrainSeq.parquet/',
    #         'genetics-portal-dev-sumstats/unfiltered/molecular_trait/GEUVADIS.parquet/']
    for inf in paths:
        if fs.isdir(inf):
            print("gs://" + inf)
            df = (
                spark.read.parquet("gs://" + inf)
                .withColumn('input_name', strip_path_mol(lit(inf)))
            )
            mol_dfs.append(df)

    # Take union
    sumstats = functools.reduce(
        functools.partial(pyspark.sql.DataFrame.unionByName, allowMissingColumns=True),
        mol_dfs
    )

    cols_to_keep = ['study_id', 'bio_feature', 'gene_id', 'chrom', 'pos', 'ref', 'alt', 'pval']
    
    # Calculate the number of tests and min pval per gene ----------
    min_pvals = (
        sumstats
        .select(*cols_to_keep)
        .groupby('study_id', 'bio_feature', 'gene_id')
        .agg(count(col('pval')).alias('num_tests'),
             min(col('pval')).alias('min_pval'))
        .orderBy('study_id', 'bio_feature', 'min_pval')
    )

    # Collect all data and write using pandas
    min_pvals.toPandas().to_csv(
        'gs://genetics-portal-dev-analysis/js29/molecular_trait/211202/min_pvals_per_gene.csv.gz',
        index=False)

    # Previous code to write with Spark
    # (
    #     min_pvals
    #     .coalesce(1)
    #     .write
    #     .mode('overwrite')
    #     .csv('gs://genetics-portal-dev-analysis/js29/molecular_trait/min_pvals_per_gene.csv.gz', nullValue='', emptyValue='', compression="gzip")
    # )

    print('Time taken: {}'.format(datetime.now() - start_time))

    return 0


if __name__ == '__main__':

    main()
