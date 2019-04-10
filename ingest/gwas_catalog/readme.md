Ingest GWAS Catalog sumstats
============================

Spark workflow to read, clean and transfrom summary stats from GWAS Catalog.

262 studies European (76.6%)
80 studies non-European (23.4%)

Of the 262:
200 studies have required fields (76.3%)
62 studies did not (23.7%)

#### Usage

```
# Get list of input files on GCS
gsutil -m ls gs://genetics-portal-raw/gwas_catalog/harmonised_190404/\*.tsv > configs/gwascatalog_inputs/gcs_input_paths.txt

# Download GWAS Catalog study table
wget -O - https://www.ebi.ac.uk/gwas/api/search/downloads/studies_alternative > configs/gwascatalog_inputs/gwascat_study_table.tsv

# Make metadata table
python 1_create_gwascatalog_metadata_table.py

# Edit metadata table manually (!) to
# 1. Remove studies that are non-European
# 2. Extract total sample size
# 3. Extract case count for case-control studies

# Get list of existing output files
gsutil -m ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt

# Create manifest file
python 2_create_ukb_manifest.py

# Start cluster (see below)

# Submit jobs to cluster
python run_all.py

# Check that its working as expected, then increase cluster number of workers

# Check outputs and any errors
```

#### Starting a Dataproc cluster

```
# Start large server
gcloud beta dataproc clusters create \
    em-ingest-gwascatalog \
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
gcloud compute ssh em-ingest-gwascatalog-m \
  --project=open-targets-genetics \
  --zone=europe-west1-d -- -D 1080 -N

"EdApplications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-ingest-gwascatalog-m" http://em-ingest-gwascatalog-m:8088
```

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc