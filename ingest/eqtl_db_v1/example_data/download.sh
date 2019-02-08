#!/usr/bin/env bash
#

mkdir -p Naranbhai_2015/array
cd Naranbhai_2015/array
gsutil cp gs://genetics-portal-raw/eqtl_db_v1/raw/Naranbhai_2015/array/neutrophil_CD16.permuted.txt.gz .
gsutil cat gs://genetics-portal-raw/eqtl_db_v1/raw/Naranbhai_2015/array/neutrophil_CD16.variant_information.txt.gz | zcat | head -100000 | bgzip -c > neutrophil_CD16.variant_information.txt.gz
gsutil cat gs://genetics-portal-raw/eqtl_db_v1/raw/Naranbhai_2015/array/neutrophil_CD16.nominal.sorted.txt.gz | zcat | head -100000 | bgzip -c > neutrophil_CD16.nominal.sorted.txt.gz
cd ../..

mkdir -p HipSci/featureCounts
cd HipSci/featureCounts
gsutil cp gs://genetics-portal-raw/eqtl_db_v1/raw/HipSci/featureCounts/iPSC.permuted.txt.gz .
gsutil cat gs://genetics-portal-raw/eqtl_db_v1/raw/HipSci/featureCounts/iPSC.variant_information.txt.gz | zcat | head -100000 | bgzip -c > iPSC.variant_information.txt.gz
gsutil cat gs://genetics-portal-raw/eqtl_db_v1/raw/HipSci/featureCounts/iPSC.nominal.sorted.txt.gz | zcat | head -100000 | bgzip -c > iPSC.nominal.sorted.txt.gz
cd ../..

gsutil cp gs://genetics-portal-raw/eqtl_db_v1/raw/HumanHT-12_V4_gene_metadata.txt .
gsutil cp gs://genetics-portal-raw/eqtl_db_v1/raw/featureCounts_Ensembl_92_gene_metadata.txt .

echo COMPLETE
