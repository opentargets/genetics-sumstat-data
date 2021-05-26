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
# Install JDK, needed for running Spark command at the end
sudo apt install openjdk-8-jdk

# Authenticate google cloud storage
gcloud auth application-default login

# Install dependencies into isolated environment
conda env create -n sumstats_data --file environment.yaml
conda activate sumstats_data
cd genetics-sumstat-data/ingest/gwas_catalog

# First download the latest sumstats to GCS (those not already completed).
# Run steps from here:
0_download_gwascatalog_to_gcs.sh

# Get list of input files on GCS
#gsutil -m ls gs://genetics-portal-dev-raw/gwas_catalog/harmonised_210223/\*.tsv.gz > configs/gwascatalog_inputs/gcs_input_paths.txt
gsutil -m ls gs://genetics-portal-dev-raw/gwas_catalog/harmonised_GCST90013791/\*.tsv.gz > configs/gwascatalog_inputs/gcs_input_paths.txt

# Download GWAS Catalog study table
wget -O - https://www.ebi.ac.uk/gwas/api/search/downloads/studies_alternative > configs/gwascatalog_inputs/gwascat_study_table.tsv
# Unpublished study - not currently used
wget -O - https://www.ebi.ac.uk/gwas/api/search/downloads/unpublished_studies > configs/gwascatalog_inputs/gwascat_unpublished_study_table.tsv

# Make metadata table
python 1_create_gwascatalog_metadata_table.py

# Edit metadata table manually (!) to
# 1. Remove studies that are non-European
# 2. Extract total sample size
# 3. Extract case count for case-control studies

# Get list of existing output files
#gsutil -m -u open-targets-genetics ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt

# Create manifest file
# This requires input file configs/gwascatalog_outputs/gwascat_metadata_curation.tsv
# Creates output file configs/gwascatalog.manifest.json
python 2_create_gwascatalog_manifest.py

# Start cluster (see below)
# Then set region
gcloud config set dataproc/region europe-west1

# Submit jobs to cluster
python run_all.py

# Check that its working as expected, then increase cluster number of workers

# Check outputs and any errors
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas_new/*/_SUCCESS" > configs/gwascatalog_outputs/ingest_completed_paths.txt
```

#### Starting a Dataproc cluster

```
# Start large server
# Do this in the cloud console, not in a VM instance
gcloud beta dataproc clusters create \
    em-ingest-gwascatalog \
    --region europe-west1 \
    --image-version=1.5-ubuntu18 \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-secondary-workers=0 \
    --worker-machine-type=c2-standard-16 \
    --num-workers=2 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=2 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=20m

# Or run single-node
gcloud beta dataproc clusters create \
    em-ingest-gwascatalog \
    --region europe-west1 \
    --image-version=1.5-ubuntu18 \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=7,spark:spark.executor.instances=1 \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --master-machine-type=n1-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m


# Replaced the image version, to be sure it runs the same as when Ed coded it
# (Older Spark version 2.4)
#    --image-version=preview \

# To monitor (run on your local machine)
# (Instructions shown by clicking on dataproc cluster in the console and selecting "Web Interfaces")
gcloud compute ssh em-ingest-gwascatalog-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/em-ingest-gwascatalog-m" http://em-ingest-gwascatalog-m:8088

# To update the number of workers
gcloud dataproc clusters update em-ingest-gwascatalog \
    --region=europe-west1 \
    --num-workers=4
```

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc