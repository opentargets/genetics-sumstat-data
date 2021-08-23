Extract windows around significant positions
============================================

Scans summary statistic files and extracts only windows around "significant" regions.

This will massively reduce the size of the input data for fine-mapping and coloc pipelines.

```
# Get list of all input parquet files
# Note that we don't need significant windows for FinnGen since we don't
# run fine-mapping ourselves.
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas/*/*.parquet/_SUCCESS" | grep -v 'FINNGEN' > gcs_input_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait/*.parquet/_SUCCESS" >> gcs_input_paths.txt

# Get list of completed files
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/*/*/*.parquet/_SUCCESS" > gcs_completed_paths.txt

# Start cluster (see below)

# Queue all, specifying output directory
version_date=`date +%y%m%d`
python queue_all.py "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/${version_date}"

# If there are many jobs, then increase the number of workers using the Dataproc UI
# Ideally a 50:50 primary to secondary worker ratio:
# https://cloud.google.com/dataproc/docs/concepts/compute/secondary-vms
```

### Cluster commands for filtering

```bash
# Start cluster
gcloud beta dataproc clusters create \
    js-sumstatfilter \
    --image-version=1.5-ubuntu18 \
    --metadata 'CONDA_PACKAGES=pandas pyarrow' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=dataproc:efm.spark.shuffle=primary-worker \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn,yarn:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
    --master-machine-type=n1-standard-8 \
    --master-boot-disk-size=1TB \
    --worker-machine-type=n1-standard-8 \
    --num-workers=2 \
    --num-secondary-workers=0 \
    --worker-boot-disk-size=1TB \
    --zone=europe-west1-d \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=10m

# Single-node
gcloud beta dataproc clusters create \
    js-sumstatfilter \
    --image-version=2.0-ubuntu18 \
    --metadata 'CONDA_PACKAGES=pandas pyarrow' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn,yarn:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=20m

# To monitor
gcloud compute ssh js-sumstatfilter-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-sumstatfilter-m" http://js-sumstatfilter-m:8088

# To update the number of workers
gcloud dataproc clusters update js-sumstatfilter \
    --region=europe-west1 \
    --project=open-targets-genetics-dev \
    --num-workers=8 \
    --num-secondary-workers=8
```

### Cluster commands for union/repartitioning

```
# Single-node (for taking union and repartitioning)
gcloud beta dataproc clusters create \
    js-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=30,spark:spark.executor.instances=1 \
    --master-machine-type=n1-highmem-32 \
    --master-boot-disk-size=2TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=10m

# Submit
gcloud dataproc jobs submit pyspark \
    --cluster=js-sumstatfilter \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    union_and_repartition_into_single_dataset.py

# To monitor
gcloud compute ssh js-sumstatfilter-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-sumstatfilter-m" http://js-sumstatfilter-m:8088

```