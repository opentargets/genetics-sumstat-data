#!/bin/bash

# First get the latest list of harmonised studies from GWAS catalog
curl -s http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt > configs/gwascatalog_inputs/ftp_paths.txt

# Download GWAS Catalog study table
wget -O - https://www.ebi.ac.uk/gwas/api/search/downloads/studies_alternative > configs/gwascatalog_inputs/gwascat_study_table.tsv
# Unpublished studies - not currently used
# wget -O - https://www.ebi.ac.uk/gwas/api/search/downloads/unpublished_studies > configs/gwascatalog_inputs/gwascat_unpublished_study_table.tsv

# Get list of studies already ingested (this step takes a few minutes)
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas/**/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt

# Get manually curated metadata table from previous release
gsutil cp 'gs://genetics-portal-dev-sumstats/unfiltered/metadata/gwas_metadata_curated.latest.tsv' 'configs/gwas_metadata_curated.latest.previous.tsv' 
