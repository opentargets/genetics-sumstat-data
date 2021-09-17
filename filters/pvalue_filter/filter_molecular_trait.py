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

    # Args
    in_path = 'gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_partitioned'
    outf = 'gs://genetics-portal-dev-sumstats/filtered/pvalue_0.05/molecular_trait/210917'
    pval_threshold = 0.05
    # study_list = [
    #     'Alasoo_2018.parquet',
    #     'BLUEPRINT.parquet',
    #     'BrainSeq.parquet',
    #     'Braineac2.parquet',
    #     'CAP.parquet',
    #     'CEDAR.parquet',
    #     'CommonMind.parquet',
    #     'FUSION.parquet',
    #     'Fairfax_2012.parquet',
    #     'Fairfax_2014.parquet',
    #     'GENCORD.parquet',
    #     'GEUVADIS.parquet',
    #     'GTEx-eQTL.parquet',
    #     'HipSci.parquet',
    #     'Kasela_2017.parquet',
    #     'Lepik_2017.parquet',
    #     'Naranbhai_2015.parquet',
    #     'Nedelec_2016.parquet',
    #     'Peng_2018.parquet',
    #     'PhLiPS.parquet',
    #     'Quach_2016.parquet',
    #     'ROSMAP.parquet',
    #     'SUN2018.parquet',
    #     'Schmiedel_2018.parquet',
    #     'Schwartzentruber_2018.parquet',
    #     'Steinberg_2020.parquet',
    #     'TwinsUK.parquet',
    #     'Young_2019.parquet',
    #     'eQTLGen.parquet',
    #     'iPSCORE.parquet',
    #     'van_de_Bunt_2015.parquet'
    # ]

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load list of datasets, filtering as we go
    # dfs = []
    # for in_path in [os.path.join(in_path, study) for study in study_list]:
    #     df_temp = (
    #         spark.read.parquet(in_path)
    #         .filter(col('pval') <= pval_threshold)
    #     )
    #     dfs.append(df_temp)
    
    # # Take union
    # df = reduce(pyspark.sql.DataFrame.unionByName, dfs)

    # Load datasets
    df = (
        spark.read.option("mergeSchema", "true")
        .parquet(in_path)
    )

    # Filter
    df = df.filter(col('pval') <= pval_threshold)

    # Rename type to type_id, and cast info to float
    df = (
        df.withColumnRenamed('type', 'type_id')
          .withColumn('info', col('info').cast(DoubleType()))
          .drop('col_study_id')
        # In the last release, we copied all data into molecular_trait_partitioned with
        # an artificial partitioning column col_study_id so that all mol_trait data
        # could be read in at once, but this isn't needed downstream.
    )
    
    # # Repartition
    # df = (
    #     df.repartitionByRange(2000, 'chrom', 'pos')
    #     .sortWithinPartitions('chrom', 'pos')
    # )

    # Save
    (
        df
        .write
        # .paritionBy('study_id')
        .parquet(
            outf,
            mode='overwrite'
        )
    )
    
    return 0

if __name__ == '__main__':

    main()
