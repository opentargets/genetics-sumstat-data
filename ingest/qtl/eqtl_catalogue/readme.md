Ingest eQTL DB sumstats
=======================

Spark workflow to read, clean and transfrom summary stats from eQTL DB.

#### Usage

```
VERSION=210824

# Download sumstats locally to sumstats_raw
1_download_sumstats.sh

# Remove duplicate rows into sumstats_dedup (specify number of cores)
NCORES=15
bash 2_remove_duplicate_rows.sh $NCORES

# Copy to GCS
gsutil -m rsync -r sumstats_dedup/ gs://genetics-portal-dev-raw/eqtl_catalogue/$VERSION/sumstats_dedup

# Get list of completed files (if any)
gsutil ls gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_temp/*.parquet/_metadata > configs/gcs_completed_paths.txt

# Create manifest file
python 3_make_manifest.py

# Start large cluster (see below)

# Submit jobs to cluster
python run_all.py

# Increase number of workers/preemptible workers through browser interface

# For unknown reasons many jobs die if they aren't on fairly large highmem machines.
```

```
# Start test cluster
gcloud beta dataproc clusters create \
    em-cluster-eqtldb-ingest \
    --image-version=2.0-ubuntu18 \
    --metadata 'CONDA_PACKAGES=scipy pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=15,spark:spark.executor.instances=1 \
    --master-machine-type=n2-highmem-16 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=2 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Start large cluster
gcloud beta dataproc clusters create \
     em-cluster-eqtldb-ingest \
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
    --num-worker-local-ssds=0 \
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

# To update the number of workers
gcloud dataproc clusters update em-cluster-eqtldb-ingest \
    --region=europe-west1 \
    --num-workers=6 \
    --num-secondary-workers=6
```

# Blueprint ran on 32 cores in 2 hr 12 min