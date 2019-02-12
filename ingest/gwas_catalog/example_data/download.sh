#!/usr/bin/env bash
#

# set -euo pipefail

# Crohns immunochip
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/LiuJZ_26192919_GCST003044/harmonised/26192919-GCST003044-EFO_0000384.h.tsv.gz
# IBD
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/deLangeKM_28067908_GCST004131/harmonised/28067908-GCST004131-EFO_0003767.h.tsv.gz
# Intelligence
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/SavageJE_29942086_GCST006250/harmonised/29942086-GCST006250-EFO_0004337.h.tsv.gz

gsutil cp -r gs://genetics-portal-staging/variant-annotation/190129/variant-annotation_af-only_chrom10.parquet .

# Unzip
gunzip *.h.tsv.gz

echo COMPLETE
