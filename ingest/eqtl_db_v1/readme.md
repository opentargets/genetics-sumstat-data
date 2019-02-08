```
# Start cluster
gcloud beta dataproc clusters create \
    em-cluster-eqtldb-ingest \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=7,spark:spark.executor.instances=1 \
    --metadata 'CONDA_PACKAGES=pandas scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-cluster-eqtldb-ingest \
    2_process.py

# To monitor
gcloud compute ssh em-cluster-eqtldb-ingest-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-eqtldb-ingest-m" http://em-cluster-eqtldb-ingest-m:8088

```
