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

    # File args GCS
    inf = 'gs://genetics-portal-staging/variant-annotation/190129/variant-annotation.parquet'
    outf = 'gs://genetics-portal-staging/variant-annotation/190129/variant-annotation_af-only_chrom10.parquet'

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    start_time = time()

    (
        spark.read.parquet(inf)
             .select('chrom_b37',
                     'pos_b37',
                     'chrom_b38',
                     'pos_b38',
                     'ref',
                     'alt',
                     'af')
            .filter(col('chrom_b37') == "10")
            .repartitionByRange('chrom_b38', 'pos_b38')
            .sortWithinPartitions('chrom_b38', 'pos_b38')
            .write.parquet(outf)
    )


    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0

if __name__ == '__main__':

    main()
