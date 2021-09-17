#!/usr/bin/env bash
#

set -euo pipefail

# Just for info, compare data sizes of the different types of quantifications
# that eQTL catalogue provide
#lftp -e "ls -l /pub/databases/spot/eQTL/csv/GEUVADIS/ge/GEUVADIS_ge_LCL.all.tsv.gz;quit" ftp.ebi.ac.uk
#lftp -e "ls -l /pub/databases/spot/eQTL/csv/GEUVADIS/exon/GEUVADIS_exon_LCL.all.tsv.gz;quit" ftp.ebi.ac.uk
#lftp -e "ls -l /pub/databases/spot/eQTL/csv/GEUVADIS/tx/GEUVADIS_tx_LCL.all.tsv.gz;quit" ftp.ebi.ac.uk
#lftp -e "ls -l /pub/databases/spot/eQTL/csv/GEUVADIS/txrev/GEUVADIS_txrev_LCL.all.tsv.gz;quit" ftp.ebi.ac.uk
#-rw-rw-r--    1 ftp      ftp      3818629309 Dec 04  2020 GEUVADIS_ge_LCL.all.tsv.gz                    
#-rw-rw-r--    1 ftp      ftp      71045596738 Dec 04  2020 GEUVADIS_exon_LCL.all.tsv.gz                     
#-rw-rw-r--    1 ftp      ftp      25377717214 Dec 04  2020 GEUVADIS_tx_LCL.all.tsv.gz                   
#-rw-rw-r--    1 ftp      ftp      46558161218 Dec 04  2020 GEUVADIS_txrev_LCL.all.tsv.gz  

if [ ! -d "$DIRECTORY" ]; then
  mkdir sumstats_raw
fi

# First get the file listing all FTP paths from eQTL catalogue
wget https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths.tsv -O configs/tabix_ftp_paths.tsv

# Select only gene expression QTL datasets
cat configs/tabix_ftp_paths.tsv | awk 'BEGIN {FS="\t"} $7 ~ /ge|microarray/' > configs/ftp_paths.gene_expression.tsv
cut -f 9 configs/ftp_paths.gene_expression.tsv > configs/ftp_paths.to_download.tsv

# Download to sumstats_raw folder
# About 500 Gb for eQTL catalogue gene expression sumstats
wget --no-clobber --input-file=configs/ftp_paths.to_download.tsv --directory-prefix=sumstats_raw
wget --no-clobber --input-file=configs/ftp_paths.to_download.missing.tsv --directory-prefix=sumstats_raw

# Copy gene metadata to GCS
wget https://zenodo.org/record/3366011/files/gene_counts_Ensembl_96_phenotype_metadata.tsv.gz
wget https://zenodo.org/record/3366011/files/HumanHT-12_V4_Ensembl_96_phenotype_metadata.tsv.gz
# Not sure if we need the Affy data - it wasn't used in previous release
wget https://zenodo.org/record/3366011/files/Affy_Human_Gene_1_0_ST_Ensembl_96_phenotype_metadata.tsv.gz
gsutil -m cp gene_counts_Ensembl_96_phenotype_metadata.tsv.gz gs://genetics-portal-dev-raw/eqtl_catalogue/210824/
gsutil -m cp HumanHT-12_V4_Ensembl_96_phenotype_metadata.tsv.gz gs://genetics-portal-dev-raw/eqtl_catalogue/210824/
gsutil cp configs/tabix_ftp_paths.tsv gs://genetics-portal-dev-raw/eqtl_catalogue/210824/

echo COMPLETE
