#!/usr/bin/env bash
#

gsutil cp gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/neale_v2_sumstats/50_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz .
gsutil cp gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/output/saige_nov2017_sumstats/PheCode_401_SAIGE_Nov2017_MACge20.tsv.gz .
wget ftp://ftp.ensembl.org/pub/grch37/current/variation/vcf/homo_sapiens/homo_sapiens-chr1.vcf.gz

echo COMPLETE
