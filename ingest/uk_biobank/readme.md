Ingest GWAS Catalog sumstats
============================

Spark workflow to read, clean and transfrom summary stats from the GWAS Catalog.

Steps:
  1. Create manifest `configs/manifest.json`
  2. Remove old versions of the output (which will cause errors)
  3. Start dataproc cluster (see below)
  4. Submit jobs `python run_all.py`
  5. Check logs for errors


TODO:
- Filter "low confidence" variants from Neale V2

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc


```
# Start server (test)
gcloud beta dataproc clusters create \
    em-ingest-ukb \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=31,spark:spark.executor.instances=1 \
    --master-machine-type=n1-standard-32 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Start large server
gcloud beta dataproc clusters create \
    em-ingest-ukb \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --preemptible-worker-boot-disk-size=1TB \
    --worker-machine-type=n1-standard-64 \
    --num-workers=3 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=10m

# To monitor
gcloud compute ssh em-ingest-ukb-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-ingest-ukb-m" http://em-ingest-ukb-m:8088
```
