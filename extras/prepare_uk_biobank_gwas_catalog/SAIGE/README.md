SAIGE Nov 2017 V3
=================

Scripts to prepare [SAIGE](https://www.dropbox.com/sh/wuj4y8wsqjz78om/AAACfAJK54KtvnzSTAoaZTLma?dl=0)  UK Biobank summary statistics for GWAS Catalog database.

### Step 1: Download data to GCS

Output dir: `gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw`

Note, there are 120 files are present in the manifest but missing from the source (dropbox). See below for a list of these.


### Step 2: Process

Needs doing:
- Add/rename standard columns
  'variant_id' = variant ID
  'p-value' = p-value
  'chromosome' = chromosome
  'base_pair_location' = base pair location
  'odds_ratio' = odds ratio
  'ci_lower' = lower 95% confidence interval
  'ci_upper' = upper 95% confidence interval
  'beta' = beta
  'standard_error' = standard error
  'effect_allele' = effect allele
  'other_allele' = other allele
  'effect_allele_frequency' = effect allele frequency
- Add other columns
  - Phenotype information?
  - Sample size
  - Case numbers
  - info
  - Others?
- Missing set to NA

```
# Start server
gcloud beta dataproc clusters create \
    em-cluster-saige \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=31,spark:spark.executor.instances=1 \
    --master-machine-type=n1-standard-32 \
    --master-boot-disk-size=1TB \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-cluster-saige \
    2_process.py

# To monitor
gcloud compute ssh em-cluster-saige-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-saige-m" http://em-cluster-saige-m:8088
```

# Files missing from dropbox

```
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_174.11_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_174.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_175_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_180.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_180.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_180_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_182_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_184.11_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_184.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_184.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_184_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_187.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_187.8_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_218.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_218.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_218_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_220_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_221_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_256.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_256_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_257.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_257_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_601.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_608_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_609.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_609_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610.8_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_610_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.31_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.32_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.33_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.51_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.52_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.53_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.54_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614.5_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_614_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_615_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_617_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_618.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_618.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_618.5_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_618.6_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_618_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619.5_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_619_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_620_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_621_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_622.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_622.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_622_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_623_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_624.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_624.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_624.9_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_624_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_625.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_625_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.11_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.12_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.13_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.14_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626.8_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_626_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_627.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_627.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_627.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_627.4_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_627_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_628_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_634.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_634.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_634_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_635.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_635.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_635_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_636.2_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_636.3_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_636_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_638_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_642.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_642_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_643.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_643_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_645_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_646_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_647.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_647_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_649.1_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_649_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_650_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_651_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_652_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_653_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_654_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_655_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_661_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_663_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_665_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_669_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_671_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_674_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_676_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_679_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_751.11_SAIGE_MACge20.txt.gz
gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw/PheCode_792_SAIGE_MACge20.txt.gz
```
