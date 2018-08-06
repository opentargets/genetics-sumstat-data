#!/usr/bin/env bash
#

set -euo pipefail

# Inputs
in_gcs=gs://uk_biobank_data/em21/neale_summary_statistics_20170915/cleaned_data/clean/*.nealeUKB_20170915.assoc.clean.tsv.gz
local_input=input_nealeukb_data
# Outputs
out_gcs=gs://genetics-portal-sumstats/gwas/genome_wide
local_output=output_nealeukb_data
# Args
ncores=31

# Make local directories
mkdir -p $local_input
mkdir -p $local_output

# Copy data to local
gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp $in_gcs $local_input

# Calc number of chroms. Speeds up computation.
mkdir -p chrom_lists
for inf in $local_input/*.nealeUKB_20170915.assoc.clean.tsv.gz; do
  outf=chrom_lists/$(basename $inf)
  outf=${outf/.nealeUKB_20170915.assoc.clean.tsv.gz/.chrom_list.txt}
  echo "zcat < $inf | tail -n +2 | cut -f 3 | uniq | sort | uniq > $outf"
done | parallel -j $ncores

# Run
snakemake --cores $ncores --snakefile nealeukb.Snakefile

# Copy output to gcs
gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp -r $local_output $out_gcs

# Shutdown instance
gcloud compute instances stop em-sumstat-processing-big --zone="europe-west1-d"

echo COMPLETE
