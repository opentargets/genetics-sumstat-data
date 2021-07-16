#!/usr/bin/env bash
#

# Download sitelist
gsutil -m cp -n gs://genetics-portal-data/variant-annotation/190129/variant-annotation.sitelist.tsv.gz .
gunzip variant-annotation.sitelist.tsv.gz

# Download manifest and ensembl id map
gsutil -m cp -n gs://genetics-portal-raw/pqtl_sun2018/raw_data/001_SOMALOGIC_GWAS_protein_info.csv .
gsutil -m cp -n gs://genetics-portal-raw/pqtl_sun2018/Sun_pQTL_uniprot_ensembl_lut.tsv .

# Download gene dictionary
gsutil -m cp -n gs://genetics-portal-data/lut/gene_dictionary.json .

# Make folder for sumstats
mkdir -p pqtls
cd pqtls

# Download IL6R.4139.71.2
mkdir -p IL6R.4139.71.2
cd IL6R.4139.71.2
gsutil -m cp -rn gs://genetics-portal-raw/pqtl_sun2018/raw_data/IL6R.4139.71.2/IL6R.4139.71.2_chrom_1_meta_final_v1.tsv.gz .
cd ..

# Download IL6R.8092.29.3
mkdir -p IL6R.8092.29.3
cd IL6R.8092.29.3
gsutil -m cp -rn gs://genetics-portal-raw/pqtl_sun2018/raw_data/IL6R.8092.29.3/IL6R.8092.29.3_chrom_1_meta_final_v1.tsv.gz .
cd ..

# Download QSOX2.8397.147.3
mkdir -p QSOX2.8397.147.3
cd QSOX2.8397.147.3
gsutil -m cp -rn gs://genetics-portal-raw/pqtl_sun2018/raw_data/QSOX2.8397.147.3/QSOX2.8397.147.3_chrom_9_meta_final_v1.tsv.gz .
cd ..

# Unzip
gunzip */*.gz


echo COMPLETE
