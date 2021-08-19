#!/usr/bin/env bash
#

gsutil -u open-targets-genetics-dev cp gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_sQTL_all_associations/Adipose_Subcutaneous.v8.EUR.sqtl_allpairs.chr21.parquet .

gsutil cp gs://genetics-portal-dev-data/dev/variant-annotation/190129/variant-annotation-sitelist-chr21.parquet .

echo COMPLETE
