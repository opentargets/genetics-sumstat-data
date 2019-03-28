Extract windows around significant positions
============================================

Scans summary statistic files and extracts only windows around significant regions.

This will massively reduce the size of the input data for fine-mapping and coloc pipelines.

I have two different methods in Spark:
  1. Using a window function. This processed 1 dataset in 39 mins on 16 cores on dataproc.
  2. Using a inner join. This processed 1 dataset in 2 min 8 sec on 16 cores on dataproc.

```
# Start cluster
gcloud beta dataproc clusters create \
    em-cluster-sumstatfilter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=15,spark:spark.executor.instances=1 \
    --master-machine-type=n1-standard-16 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

# Submit to cluster (test run)
gcloud dataproc jobs submit pyspark \
    --cluster=em-cluster-sumstatfilter \
    filter_by_window.py -- --in_sumstats gs://genetics-portal-sumstats2/gwas_2/GCST004131_ibd.parquet --out_sumstats gs://genetics-portal-sumstats2/window_filtered/gwas_2/GCST004131_ibd.parquet --window 2000000 --pval 5e-8 --data_type gwas

# To monitor
gcloud compute ssh em-cluster-sumstatfilter-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-cluster-sumstatfilter-m" http://em-cluster-sumstatfilter-m:8088
```