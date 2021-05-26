Ingest GWAS Catalog sumstats
============================

Spark workflow to read, clean and transform summary stats from UK Biobank.

#### Usage

```

# Get list of input files on GCS
gsutil -m ls gs://genetics-portal-raw/uk_biobank_sumstats/harmonised_split_190318/\*.GRCh38.tsv.split000.gz > configs/ukb_inputs/gcs_input_paths.txt

# Get list of existing output files
gsutil ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/ukb_inputs/gcs_completed_paths.txt

# Create manifest file
python create_ukb_manifest.py

# Start cluster (see below)

# Submit jobs to cluster
python run_all.py

# Check that it's working as expected, then increase cluster number of workers


# Check outputs and any errors
```

#### Starting a Dataproc cluster

```
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
    --worker-machine-type=n1-standard-16 \
    --num-workers=2 \
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

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc