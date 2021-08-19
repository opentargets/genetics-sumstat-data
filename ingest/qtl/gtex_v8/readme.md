Ingest GTEx v8 splice QTL sumstats
=======================

Spark workflow to read, clean and transfrom splicing QTL summary stats from GTEx.

Note: We import eQTLs from eQTL catalogue, which uniformly processed GTEx as well as other datasets. But we get splicing QTLs directly from GTEx, which were computed using LeafCutter (https://www.nature.com/articles/s41588-017-0004-9). This is because for splicing/transcript isoforms the eQTL catalogue uses methods that produce far more QTL data (at least 20x more than eQTLs) but without evidence of having better performance.

Each gene can have multiple splicing clusters, and each cluster has multiple splicing junctions. The p-value associations for the different junctions for a cluster are largely redundant, and one junction will be the "cleanest" i.e. have the lowest p values. So we keep only one junction per splicing cluster.

#### Usage

```
# Note: sumstats for GTEx are already on GCS:
# https://console.cloud.google.com/storage/browser/gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_sQTL_all_associations
# But these are requester pays buckets, and I haven't figured out how to access
# them directly from Dataproc.
# Copy GTEx sumstats to our own bucket
gsutil -m -u open-targets-genetics-dev rsync -r gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_sQTL_all_associations gs://genetics-portal-dev-raw/gtex_v8/GTEx_Analysis_v8_EUR_sQTL_all_associations

# Create manifest file
python 1_make_manifest.py

# Start large cluster (see below)

# Submit jobs to cluster
python run_all.py

# Increase number of preemptible workers through browser interface

# When all done and verified, then delete GTEx sumstats from our bucket
gsutil -m rm -r gs://genetics-portal-dev-raw/gtex_v8/GTEx_Analysis_v8_EUR_sQTL_all_associations
```



```
NCORES=16
N_EXEC=$((NCORES-1))
# Start single-node cluster
gcloud beta dataproc clusters create \
    js-cluster-gtex8-ingest \
    --image-version=2.0-ubuntu18 \
    --metadata 'CONDA_PACKAGES=pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=$N_EXEC,spark:spark.executor.instances=1 \
    --master-machine-type=n1-highmem-$NCORES \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Or start large cluster
gcloud beta dataproc clusters create \
    js-cluster-gtex8-ingest \
    --image-version=1.5-ubuntu18 \
    --metadata 'CONDA_PACKAGES=scipy pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=dataproc:efm.spark.shuffle=primary-worker \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n2-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-secondary-workers=0 \
    --worker-machine-type=n2-highmem-8 \
    --num-workers=2 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=30m


# To monitor
gcloud compute ssh js-cluster-gtex8-ingest-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-cluster-gtex8-ingest-m" http://js-cluster-gtex8-ingest-m:8088
```