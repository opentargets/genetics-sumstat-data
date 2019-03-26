#!/usr/bin/env bash
#


gsutil cp gs://genetics-portal-data/lut/biofeature_lut_190208.json .
gsutil cp gs://genetics-portal-raw/eqtl_gtex_v7/gtex7_sample_sizes.tsv .
gsutil cp gs://genetics-portal-data/variant-annotation/190129/variant-annotation.parquet/part-00000-2f1d26b6-a5b6-428f-9e62-affcd1ef6971-c000.snappy.parquet varindex_part-00000-2f1d26b6-a5b6-428f-9e62-affcd1ef6971-c000.snappy.parquet


mkdir gtex
cd gtex

gsutil cat gs://genetics-portal-raw/eqtl_gtex_v7/allpairs/Adipose_Subcutaneous.allpairs.txt.gz | zcat | head -1000000 > Adipose_Subcutaneous.allpairs.tsv
gsutil cat gs://genetics-portal-raw/eqtl_gtex_v7/allpairs/Brain_Cortex.allpairs.txt.gz | zcat | head -1000000 > Brain_Cortex.allpairs.tsv
gsutil cat gs://genetics-portal-raw/eqtl_gtex_v7/allpairs/Liver.allpairs.txt.gz | zcat | head -1000000 > Liver.allpairs.tsv

echo COMPLETE
