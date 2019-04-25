#!/usr/bin/env bash

mkdir gwas
cd gwas
gsutil -m cp -r gs://genetics-portal-sumstats-b38/unfiltered/gwas/GCST000568.parquet .
gsutil -m cp -r gs://genetics-portal-sumstats-b38/unfiltered/gwas/NEALE2_50_raw.parquet .
cd ..

mkdir molecular_trait
cd molecular_trait
gsutil -m cp -r gs://genetics-portal-sumstats-b38/unfiltered/molecular_trait/SUN2018.parquet .
mkdir -p SCHWARTZENTRUBER_2018.parquet/bio_feature=SENSORY_NEURON
cd SCHWARTZENTRUBER_2018.parquet/bio_feature=SENSORY_NEURON
gsutil -m cp -r gs://genetics-portal-sumstats-b38/unfiltered/molecular_trait/SCHWARTZENTRUBER_2018.parquet/bio_feature=SENSORY_NEURON/chrom=22 .
cd ../..
