#!/usr/bin/env bash
#

set -euo pipefail

lftp ftp-private.ebi.ac.uk
set ftp:ssl-force yes
set ftp:ssl-protect-data no
set ssl:verify-certificate false
login <redacted>
# Password: <redacted>

# Download to local
mirror upload/eqtls

# Copy to GCS
gsutil -m rsync -rn eqtls/ gs://genetics-portal-raw/eqtl_db_v1/raw

# Copy gene meta data
wget -O - https://github.com/kauralasoo/RNAseq_pipeline/raw/master/metadata/gene_metadata/featureCounts_Ensembl_92_gene_metadata.txt.gz | zcat | gsutil cp - gs://genetics-portal-raw/eqtl_db_v1/raw/featureCounts_Ensembl_92_gene_metadata.txt
wget -O - https://github.com/kauralasoo/RNAseq_pipeline/raw/master/metadata/gene_metadata/HumanHT-12_V4_gene_metadata.txt.gz | zcat | gsutil cp - gs://genetics-portal-raw/eqtl_db_v1/raw/HumanHT-12_V4_gene_metadata.txt

echo COMPLETE
