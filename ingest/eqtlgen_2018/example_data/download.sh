#!/usr/bin/env bash
#

# Download sitelist
gsutil -m cp -n gs://genetics-portal-data/variant-annotation/190129/variant-annotation.sitelist.tsv.gz .
gunzip variant-annotation.sitelist.tsv.gz

# Download eqtlgen
gsutil cat gs://genetics-portal-raw/eqtlgen_20180905/cis-eQTLs_full_20180905.txt.gz | zcat | head -10000 > cis-eQTLs_full_20180905.head.txt
gsutil cp gs://genetics-portal-raw/eqtlgen_20180905/2018-07-18_SNP_AF_for_AlleleB_combined_allele_counts_and_MAF_pos_added.txt .

echo COMPLETE
