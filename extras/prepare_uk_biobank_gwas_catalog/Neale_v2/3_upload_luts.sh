#!/usr/bin/env bash
#

set -euo pipefail

# Upload variant index
gsutil -m rsync -r \
  2_make_variant_index/variants.nealev2.parquet \
  gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/variants.nealev2.parquet

# Upload phenotype files
gsutil -m cp \
  1_stream_to_gcs/manifest/phenotypes.both_sexes.filtered.tsv \
  gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/phenotypes.both_sexes.filtered.tsv




echo COMPLETE
