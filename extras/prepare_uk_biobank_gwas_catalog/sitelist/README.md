UK Biobank sitelist map
=======================

Maps UK biobank variants to GRCh38 and adds RSIDs from Ensembl.



## Start dataproc spark server
```
# Start server
gcloud beta dataproc clusters create \
    em-ukb-sitelist-annotation \
    --image-version=1.2-deb9 \
    --metadata=MINICONDA_VERSION=4.4.10,JAR=gs://hail-common/builds/0.2/jars/hail-0.2-07b91f4bd378-Spark-2.2.0.jar,ZIP=gs://hail-common/builds/0.2/python/hail-0.2-07b91f4bd378.zip \
    --properties=spark:spark.driver.memory=41g,spark:spark.driver.maxResultSize=0,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,hdfs:dfs.replication=1 \
    --initialization-actions=gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://hail-common/cloudtools/init_notebook1.py \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=100GB \
    --num-master-local-ssds=0 \
    --num-preemptible-workers=0 \
    --num-worker-local-ssds=0 \
    --num-workers=3 \
    --preemptible-worker-boot-disk-size=40GB \
    --worker-boot-disk-size=40 \
    --worker-machine-type=n1-standard-8 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=10m

# Get lastest hash
HASH=$(gsutil cat gs://hail-common/builds/0.2/latest-hash/cloudtools-3-spark-2.2.0.txt)

# Submit to cluster
gcloud dataproc jobs submit pyspark \
  --cluster=em-ukb-sitelist-annotation \
  --files=gs://hail-common/builds/0.2/jars/hail-0.2-$HASH-Spark-2.2.0.jar \
  --py-files=gs://hail-common/builds/0.2/python/hail-0.2-$HASH.zip \
  --properties="spark.driver.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar,spark.executor.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar" \
  2_annotate.py

# To monitor
gcloud compute ssh em-ukb-sitelist-annotation-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-ukb-sitelist-annotation-m" http://em-ukb-sitelist-annotation-m:8088

```
