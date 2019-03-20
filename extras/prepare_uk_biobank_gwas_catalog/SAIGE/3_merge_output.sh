#!/usr/bin/env bash
#

# set -euo pipefail

instance_name=em-merge-saige
instance_zone=europe-west1-d
cores=64

in_dir="gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/output/saige_nov2017_sumstats_temp"
out_dir="gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/output/saige_nov2017_sumstats2"

for in_gcs in $(gsutil ls $in_dir | grep 'phenotype='); do

  # Get pheno name
  bn=$(basename $in_gcs)
  pheno=${bn/phenotype=/}

  # Make input and output file name
  in_path="$in_gcs""part-*.csv.gz"
  out_path=$out_dir/$pheno"_SAIGE_Nov2017_MACge20.tsv.gz"

  # Process
  echo "gsutil cat \"$in_path\" | zcat | pypy3 clean_headers.py | gzip -c | gsutil cp -n - $out_path"

done | parallel --progress -j $cores

gcloud compute instances stop $instance_name --zone=$instance_zone

echo COMPLETE
