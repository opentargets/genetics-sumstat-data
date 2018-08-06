#!/usr/bin/env bash
#

set -euo pipefail

# Inputs
# in_gcs=gs://genetics-portal-raw/pqtl_sun2018/raw_data/A1CF.12423.38.3
in_gcs=gs://genetics-portal-raw/pqtl_sun2018/raw_data/*
local_input=input_sun2018_data
# Outputs
out_gcs=gs://genetics-portal-sumstats/molecular_qtl/pqtl
local_output=output_sun2018_data
# Args
ncores=31
instance_name="em-pqtl-sumstats"
instance_zone="europe-west1-d"

# Make local directories
mkdir -p $local_input
mkdir -p $local_output

# Copy data to local
# gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp -r $in_gcs $local_input

# Run
snakemake --cores $ncores --snakefile sun_pqtl.Snakefile

# Copy output to gcs
gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp -r $local_output $out_gcs

# Shutdown instance
gcloud compute instances stop $instance_name --zone=$instance_zone

echo COMPLETE
