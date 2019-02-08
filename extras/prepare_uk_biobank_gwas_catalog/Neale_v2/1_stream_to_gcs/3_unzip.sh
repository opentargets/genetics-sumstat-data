#!/usr/bin/env bash
#

# set -euo pipefail

instance_name=em-unzip
instance_zone=europe-west1-d

for in_path in $(gsutil ls "gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/*.tsv.gz"); do
  # Make new path
  old_dir=$(dirname $in_path)
  old_file=$(basename $in_path)
  new_path=${old_dir/\/raw/\/raw_unzipped}/${old_file/.tsv.gz/.tsv}
  # Unzip cmd
  echo "gsutil cp $in_path - | zcat | gsutil cp -n - $new_path"
done | parallel -j 16

gcloud compute instances stop $instance_name --zone=$instance_zone

echo COMPLETE
