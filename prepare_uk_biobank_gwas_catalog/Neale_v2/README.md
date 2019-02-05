Neale lab version 2
===================

Scripts to prepare [Neale lab V2](http://www.nealelab.is/uk-biobank)  UK Biobank summary statistics for GWAS Catalog database. ICD traits will not be included, as these will be handles by SAIGE results.

### Step 1: Download data to GCS

Output dir: `gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw`

### Step 2: Process data

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

Requirements:
  - Spark = 2.4.0
  - Pyspark


#### Start cluster

Need to use dataproc preview version https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions?hl=hu

```
# Start server
gcloud beta dataproc clusters create \
    em-cluster-neale \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=31,spark:spark.executor.instances=1 \
    --metadata 'CONDA_PACKAGES=pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --master-machine-type=n1-standard-32 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-cluster-neale \
    4_process_v4.py

# To monitor
gcloud compute ssh em-cluster-neale-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-neale-m" http://em-cluster-neale-m:8088

```

### 3. Write to a single file for each phenotype

```
# header
phenotype	chromosome	base_pair_location	other_allele	effect_allele	variant	minor_allele	minor_AF	low_confidence_variant	n_complete_samples	AC	ytx	beta	standard_error	tstat	p-value	expected_min_category_minor_AC	expected_case_minor_AC	variant_id	info	description	source	n_non_missing	n_missing	n_controls	n_cases	logOR	logORse	odds_ratio	ci_lower	ci_upper	effect_allele_frequency]
```

# Speeds

Test data
No partitioning df + no persist 2nd join: 54
No partitioning variants + no persist: 25

Cluster (16 cores)
No partitioning variants (no cache) or df: 267
No partitioning variants (cached) or df: 270 # Using this
Partitioning variants (cached), not df: 278
Partitioning variants (cached) and df: 350
No partitioning variants (no cache) or df, no compression: 259

8 cores: 518
32 cores: 161

### Old

```
gcloud beta dataproc clusters create \
    em-cluster \
    --image-version=preview \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1,spark:spark.debug.maxToStringFields=100 \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=100GB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --num-worker-local-ssds=0 \
    --num-workers=2 \
    --preemptible-worker-boot-disk-size=40GB \
    --worker-boot-disk-size=40 \
    --worker-machine-type=n1-highmem-8 \
    --zone=europe-west1-b \
    --initialization-action-timeout=20m \
    --max-idle=15m
```
