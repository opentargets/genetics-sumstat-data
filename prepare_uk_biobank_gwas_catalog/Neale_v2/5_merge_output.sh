#!/usr/bin/env bash
#

# set -euo pipefail

instance_name=em-merge-neale
instance_zone=europe-west1-d

in_pattern="gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats_temp2/*.header/_SUCCESS"
out_dir="gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats"

for in_gcs in $(gsutil ls $in_pattern); do

  echo "Processing ", $in_gcs 

  # Get header directory
  header_dir=${in_gcs/\/_SUCCESS/}
  # Get data directory
  data_dir=${header_dir/.header/}
  # Get pheno name
  pheno=$(basename $data_dir)
  # Make input paths
  in_header=$header_dir"/part-00000"
  in_data=$data_dir"/part-*.csv.gz"
  # Make output paths
  out_path=$out_dir/$pheno".neale2.gwas.imputed_v3.both_sexes.tsv.gz"
  out_path_local=$(basename $out_path)

  # Make file locally
  gsutil cat $in_header | python3 transpose_header.py | gzip -c > $out_path_local
  gsutil cat $in_data >> $out_path_local

  # Copy to remote
  gsutil -m cp -n $out_path_local $out_path

  # Remove local
  rm $out_path_local

  # { (gsutil cat $in_header | python transpose_header.py | gzip -c) & \
  #   (gsutil cat $in_data); } | gsutil cp -n - $out_path

done

gcloud compute instances stop $instance_name --zone=$instance_zone

echo COMPLETE
