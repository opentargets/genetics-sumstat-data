Extract windows around significant positions
============================================

Scans summary statistic files and extracts only windows around "significant" regions.

This will massively reduce the size of the input data for fine-mapping and coloc pipelines.

```
# Get list of all input parquet files
gsutil -m ls "gs://genetics-portal-sumstats-b38/unfiltered/*/*.parquet/_SUCCESS" > gcs_input_paths.txt

# Get list of completed files
gsutil -m ls "gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/*/*.parquet/_SUCCESS" > gcs_completed_paths.txt

# Start cluster (see below)

# Queue all
python queue_all.py

```

### Cluster commands for filtering

```bash
# Start cluster
gcloud beta dataproc clusters create \
    em-sumstatfilter \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=pandas pyarrow' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn,yarn:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-preemptible-workers=0 \
    --worker-machine-type=n1-standard-8 \
    --num-workers=2 \
    --worker-boot-disk-size=1TB \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=10m

# Single-node
gcloud beta dataproc clusters create \
    em-sumstatfilter \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=pandas pyarrow' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn,yarn:yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator \
    --master-machine-type=n1-highmem-32 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# To monitor
gcloud compute ssh em-sumstatfilter-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-sumstatfilter-m" http://em-sumstatfilter-m:8088
```

### Cluster commands for union/repartitioning

```
# Single-node (for taking union and repartitioning)
gcloud beta dataproc clusters create \
    em-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=16,spark:spark.executor.instances=1 \
    --master-machine-type=n1-highmem-16 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit
gcloud dataproc jobs submit pyspark \
    --cluster=em-sumstatfilter \
    union_and_repartition_into_single_dataset.py

# To monitor
gcloud compute ssh em-sumstatfilter-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-sumstatfilter-m" http://em-sumstatfilter-m:8088
```