Filter summary statistics by pvalue
===================================

```bash

# Single-node
gcloud beta dataproc clusters create \
    em-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100 \
    --master-machine-type=n1-standard-64 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster
gcloud dataproc jobs submit pyspark \
    --cluster=em-sumstatfilter \
    filter_gwas.py
gcloud dataproc jobs submit pyspark \
    --cluster=em-sumstatfilter \
    filter_molecular_trait.py

# To monitor
gcloud compute ssh em-sumstatfilter-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-sumstatfilter-m" http://em-sumstatfilter-m:8088
```