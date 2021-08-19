#!/usr/bin/env bash
#

# set -euo pipefail

# Get allele freqs
gsutil cp -r gs://genetics-portal-staging/variant-annotation/190129/variant-annotation_af-only_chrom10.parquet .

mkdir -p sumstats
cd sumstats

# Crohns immunochip
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/LiuJZ_26192919_GCST003044/harmonised/26192919-GCST003044-EFO_0000384.h.tsv.gz
# deLange IBD
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/deLangeKM_28067908_GCST004131/harmonised/28067908-GCST004131-EFO_0003767.h.tsv.gz
# deLange Crohns
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/deLangeKM_28067908_GCST004132/harmonised/28067908-GCST004132-EFO_0000384.h.tsv.gz
# deLange UC
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/deLangeKM_28067908_GCST004133/harmonised/28067908-GCST004133-EFO_0000729.h.tsv.gz
# Heel bone mineral density
wget ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/KempJP_28869591_GCST006288/harmonised/28869591-GCST006288-EFO_0009270.h.tsv.gz

(zcat 28067908-GCST004132-EFO_0000384.h.tsv.gz | head -n 1; zcat 28067908-GCST004132-EFO_0000384.h.tsv.gz | awk '$3 ~ /10/') | gzip > 28067908-GCST004132-EFO_0000384.h.chr10.tsv.gz

gsutil cp 28067908-GCST004132-EFO_0000384.h.tsv.gz gs://genetics-portal-raw/gwas_catalog/harmonised_test/

cd ..

echo COMPLETE
