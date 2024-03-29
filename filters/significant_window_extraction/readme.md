Extract windows around significant positions
============================================

Scans summary statistic files and extracts only windows around "significant" regions. This massively reduces the size of the input data for fine-mapping and coloc pipelines.

If running on a new batch of ingested sumstats, then use the folder where the new sumstats were ingested. Since in general we don't want to re-run fine-mapping for all past studies, it's best to only extract significant windows for new studies as well. Downstream in the fine-mapping pipeline we can then identify (by date) the studies that still need to be fine-mapped.

```
# If recalling windows for ALL sumstats, then list all ingested sumstat files
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait/*.parquet/_SUCCESS" > gcs_input_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas/*.parquet/_SUCCESS" >> gcs_input_paths.txt

# If using just a new folder of sumstats, then update the paths below
# Paths for any new GWAS studies
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas_220217/*.parquet/_SUCCESS" > gcs_input_paths.txt
# Add paths for any new QTL studies
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait/GTEx-sQTL.parquet/_SUCCESS" >> gcs_input_paths.txt

# Get list of completed files
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/molecular_trait/*.parquet/_SUCCESS" > gcs_completed_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/*.parquet/_SUCCESS" >> gcs_completed_paths.txt

# Start cluster (see below)

# Queue all, specifying output directory
# (You may want to tmux first if there are thousands of GWAS to run,
# in case it stops submitting part way and you don't know which are
# already queued.)
python queue_all.py "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb" "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_intervals"

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
    --master-machine-type=n2-standard-8 \
    --master-boot-disk-size=1TB \
    --worker-machine-type=n2-standard-8 \
    --num-workers=4 \
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

### Commands for union/repartitioning

(This took 1.5 hrs on last run.)

```
# Get list of completed significant window files
gsutil -m ls -d "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/molecular_trait/*.parquet" > gcs_completed_moltraits.txt
gsutil -m ls -d "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/*.parquet" > gcs_completed_gwas.txt

# Single-node (for taking union and repartitioning)
gcloud beta dataproc clusters create \
    js-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=25g,spark:spark.executor.memory=25g,spark:spark.executor.cores=7,spark:spark.executor.instances=8 \
    --master-machine-type=n2-standard-64 \
    --master-boot-disk-size=2TB \
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