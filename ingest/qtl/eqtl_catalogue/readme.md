Ingest eQTL DB sumstats
=======================

Spark workflow to read, clean and transfrom summary stats from eQTL DB.

#### Usage

```
# Obtain summary stats
1_download_sumstats.sh
# Split into e.g. 100 chunks or unzip

# Create manifest file
python 2_make_manifest.py

# Start large cluster (see below)

# Submit jobs to cluster
python run_all.py

# Increase number of preemptible workers through browser interface

```

```
# Start test cluster
gcloud beta dataproc clusters create \
    em-cluster-eqtldb-ingest \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=scipy pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=62,spark:spark.executor.instances=1 \
    --master-machine-type=n2-highmem-64 \
    --master-boot-disk-size=2TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Start large cluster
gcloud beta dataproc clusters create \
     em-cluster-eqtldb-ingest \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=scipy pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n2-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --worker-machine-type=n2-highmem-8 \
    --num-workers=10 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=10m

# To monitor
gcloud compute ssh em-cluster-eqtldb-ingest-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-eqtldb-ingest-m" http://em-cluster-eqtldb-ingest-m:8088
```

# Blueprint ran on 32 cores in 2 hr 12 min