#!/usr/bin/env bash
#
# Warning: requires XX Gb ram per core to process 1 GTEx file

set -euo pipefail

# Inputs
in_gcs=gs://genetics-portal-raw/eqtl_gtex_v7/allpairs
local_input=input_gtex7_data
# Outputs
out_gcs=gs://genetics-portal-sumstats/molecular_qtl/eqtl/GTEX7
local_output=output_gtex7_data
# Args
ncores=1
# available_ram_gb=
instance_name="em-gtex-sumstats"
instance_zone="europe-west1-d"

# Make local directories
mkdir -p $local_input
mkdir -p $local_output

# Copy data to local
# gsutil -m rsync -r -x ".*DS_Store$" $in_gcs $local_input

# Run
snakemake --cores $ncores --snakefile gtex7.Snakefile

# Copy output to gcs
# gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M rsync -r -x ".*DS_Store$" $local_output $out_gcs

# Shutdown instance
# gcloud compute instances stop $instance_name --zone=$instance_zone

echo COMPLETE
