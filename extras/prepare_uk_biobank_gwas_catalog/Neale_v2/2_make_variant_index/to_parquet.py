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

def main():

    # Args
    inf = 'variants.tsv'
    outf = 'variants.nealev2.parquet'


    # Make spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)

    # Make schema for import
    import_schema = (
        StructType()
        .add('variant', StringType(), False)
        .add('chr', StringType(), False)
        .add('pos', LongType(), False)
        .add('ref', StringType(), False)
        .add('alt', StringType(), False)
        .add('rsid', StringType(), True)
        .add('varid', StringType(), True)
        .add('consequence', StringType(), True)
        .add('consequence_category', StringType(), True)
        .add('info', DoubleType(), True)
        .add('call_rate', DoubleType(), True)
        .add('AC', IntegerType(), True)
        .add('AF', DoubleType(), True)
        .add('minor_allele', StringType(), True)
        .add('minor_AF', DoubleType(), True)
        .add('p_hwe', DoubleType(), True)
        .add('n_called', IntegerType(), True)
        .add('n_not_called', IntegerType(), True)
        .add('n_hom_ref', IntegerType(), True)
        .add('n_het', IntegerType(), True)
        .add('n_hom_var', IntegerType(), True)
        .add('n_non_ref', IntegerType(), True)
        .add('r_heterozygosity', DoubleType(), True)
        .add('r_het_hom_var', StringType(), True)
        .add('r_expected_het_frequency', DoubleType(), True)
    )

    # Load
    df = spark.read.csv(
        inf,
        schema=import_schema,
        header=True,
        sep='\t'
    )

    # Show schema
    # df.printSchema()
    # df.show(3)

    # Repartition
    df = df.repartitionByRange('chr', 'pos').sort('pos')
    print('Num partitions: ', df.rdd.getNumPartitions())
    # df.explain(True)

    # Write
    ( df.write.parquet(
          outf,
          mode='overwrite',
          compression='snappy')
    )

    # # Write partitioned by chr
    # ( df.write.parquet(
    #       outf.replace('.parquet', '.partitioned.parquet'),
    #       mode='overwrite',
    #       partitionBy=['chr'],
    #       compression='snappy')
    # )

    return 0

if __name__ == '__main__':

    main()
