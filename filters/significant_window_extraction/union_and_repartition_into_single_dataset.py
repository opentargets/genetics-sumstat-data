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
    basepath = 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/'
    gwas_path1 = basepath + '210814/gwas/*.parquet'
    gwas_path2 = basepath + '210819/gwas/*.parquet'

    #in_gwas_pattern = 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/*/gwas/*.parquet'
    #in_mol_path = 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/*/molecular_trait/*.parquet'
    outf = 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_union'
    n_parts = 1000
    molecular_trait_list = [
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Alasoo_2018.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/BLUEPRINT.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/BrainSeq.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Braineac2.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/CAP.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/CEDAR.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/CommonMind.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/FUSION.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Fairfax_2012.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Fairfax_2014.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/GENCORD.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/GEUVADIS.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/GTEx-eQTL.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/HipSci.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Kasela_2017.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Lepik_2017.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Naranbhai_2015.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Nedelec_2016.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Peng_2018.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/PhLiPS.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Quach_2016.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/ROSMAP.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/SUN2018.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Schmiedel_2018.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Schwartzentruber_2018.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Steinberg_2020.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/TwinsUK.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/Young_2019.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/eQTLGen.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/iPSCORE.parquet',
        'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210901/molecular_trait/van_de_Bunt_2015.parquet'
    ]

    #
    # Load molecular trait datasets -------------------------------------------
    #
    dfs = []
    for in_path in molecular_trait_list:
        df_temp = spark.read.parquet(in_path)
        dfs.append(df_temp)
    
    # Take union
    mol_df = reduce(pyspark.sql.DataFrame.unionByName, dfs)

    #
    # Load GWAS datasets ------------------------------------------------------
    #
    # dfs = []
    # for in_path in gwas_path_list:
    #     df_temp = spark.read.parquet(in_path)
    #     dfs.append(df_temp)
    # gwas_df = reduce(pyspark.sql.DataFrame.unionByName, dfs)
    # #gwas_df = spark.read.parquet('gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/210814/gwas/FINNGEN_R5_AB1_AMOEBIASIS.parquet')

    gwas_df1 = spark.read.parquet(gwas_path1)
    gwas_df2 = spark.read.parquet(gwas_path2)
    
    #
    # Take union --------------------------------------------------------------
    #
    merged_df = (
        gwas_df1
        .unionByName(gwas_df2)
        .unionByName(mol_df.drop('num_tests'))
    )
    
    # Repartition
    merged_df = (
        merged_df.repartitionByRange(n_parts, 'chrom', 'pos')
        .sortWithinPartitions('chrom', 'pos')
    )

    # Save
    (
        merged_df
        .write.parquet(
            outf,
            mode='overwrite'
        )
    )
    
    return 0

if __name__ == '__main__':

    main()
