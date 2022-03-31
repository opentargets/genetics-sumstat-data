Filter summary statistics by pvalue
===================================

Note: Currently we only apply this to the full set of sumstats each time, which is duplicating the compute with each release. Potentially we could only filter new studies, using something similar to what is described here: https://stackoverflow.com/a/68569465

Instructions:
1. Ensure that all GWAS have been put in the root GWAS folder (gs://genetics-portal-dev-sumstats/unfiltered/gwas), if new GWAS have been ingested
2. Edit "Args" in `filter_gwas.py`
3. Edit paths as needed in `filter_molecular_trait.py`
4. Run (see below)


## Run

```bash
# Single-node
gcloud beta dataproc clusters create \
    js-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=25g,spark:spark.executor.memory=25g,spark:spark.executor.cores=7,spark:spark.executor.instances=8 \
    --master-machine-type=n2-standard-64 \
    --master-boot-disk-size=2TB \
    --num-master-local-ssds=0 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=20m \
    --project=open-targets-genetics-dev \
    --region=europe-west1

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=js-sumstatfilter \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    filter_gwas.py

gcloud dataproc jobs submit pyspark \
    --cluster=js-sumstatfilter \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    filter_molecular_trait.py

# --properties spark.submit.deployMode=cluster \

# To monitor
gcloud compute ssh js-sumstatfilter-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-sumstatfilter-m" http://js-sumstatfilter-m:8088
```