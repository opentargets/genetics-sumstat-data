# you will need to execute this successfully
# gsutil -m cp -r  gs://genetics-portal-sumstats-b38/unfiltered/gwas/FINNGEN_AB1_INTESTINAL_INFECTIONS.parquet .

import pyspark
import pyspark.sql
import pyspark.sql.functions
import pyspark.sql.functions as F
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    pyspark.sql.SparkSession.builder
        .master('local[*]')
        .getOrCreate()
)

finngen_test = ( spark.read.parquet("FINNGEN_AB1**/*.parquet"))

finngen_test.show()
print(finngen_test.where(F.col("chrom") == 1).count())
print(finngen_test.count())

finngen_test.printSchema()