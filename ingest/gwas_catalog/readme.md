Ingest GWAS Catalog sumstats
============================

Spark workflow to read, clean and transfrom summary stats from the GWAS Catalog.

Steps:
  1. Create manifest `configs/manifest.json`
  2. Remove old versions of the output (which will cause errors)
  3. Start dataproc cluster (see below)
  4. Submit jobs `python run_all.py`
  5. Check logs for errors

```
# Start server
gcloud beta dataproc clusters create \
    em-ingest-gwascatalog \
    --image-version=preview \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100 \
    --master-machine-type=n1-standard-32 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# To monitor
gcloud compute ssh em-ingest-gwascatalog-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-ingest-gwascatalog-m" http://em-ingest-gwascatalog-m:8088
```
