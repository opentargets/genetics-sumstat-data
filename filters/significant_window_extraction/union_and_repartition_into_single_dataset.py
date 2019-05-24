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

import os
import sys
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce

def main():

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Args
    in_gwas_pattern = 'gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/gwas/*.parquet'
    in_mol_path = 'gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/molecular_trait'
    outf = 'gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/union'
    n_parts = 800
    molecular_trait_list = [
        'ALASOO_2018.parquet',
        'Blueprint.parquet',
        'CEDAR.parquet',
        'FAIRFAX_2012.parquet',
        'FAIRFAX_2014.parquet',
        'GENCORD.parquet',
        'GEUVADIS.parquet',
        'GTEX_v7.parquet',
        'HIPSCI.parquet',
        'NARANBHAI_2015.parquet',
        'NEDELEC_2016.parquet',
        'QUACH_2016.parquet',
        'SCHWARTZENTRUBER_2018.parquet',
        'SUN2018.parquet',
        'TWINSUK.parquet',
        'VAN_DE_BUNT_2015.parquet',
        'eQTLGen.parquet'
    ]

    #
    # Load molecular trait datasets -------------------------------------------
    #

    # Load list of datasets
    dfs = []
    for in_path in [os.path.join(in_mol_path, study) for study in molecular_trait_list]:
        df_temp = spark.read.parquet(in_path)
        dfs.append(df_temp)
    
    # Take union
    mol_df = reduce(pyspark.sql.DataFrame.unionByName, dfs)

    #
    # Load GWAS datasets ------------------------------------------------------
    #

    # Load
    gwas_df = spark.read.parquet(in_gwas_pattern)

    #
    # Take union --------------------------------------------------------------
    #

    df = gwas_df.unionByName(
        mol_df.drop('num_tests')
    )
    
    # Repartition
    df = (
        df.repartitionByRange(n_parts, 'chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Save
    (
        df
        .write.parquet(
            outf,
            mode='overwrite'
        )
    )
    
    return 0

if __name__ == '__main__':

    main()
