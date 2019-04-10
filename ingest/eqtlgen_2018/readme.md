```
# Start cluster
gcloud beta dataproc clusters create \
    em-cluster-eqtlgen-ingest \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=31,spark:spark.executor.instances=1 \
    --master-machine-type=n1-standard-16 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-cluster-eqtlgen-ingest \
    process.py

# To monitor
gcloud compute ssh em-cluster-eqtlgen-ingest-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-eqtlgen-ingest-m" http://em-cluster-eqtlgen-ingest-m:8088
```