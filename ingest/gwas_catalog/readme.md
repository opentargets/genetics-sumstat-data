Ingest GWAS Catalog sumstats
============================

Spark workflow to read, clean and transfrom summary stats from GWAS Catalog.

Latest updates:
- V7: 
- V6: 1301 new studies with sumstats and EUR ancestry

### Usage
```
conda activate sumstats # Env created in root of genetics-sumstat-data repo

version_date=`date +%y%m%d`
version_date='220217' # You may want to fix the version date if running over more than one day

# Get latest tables from GWAS catalog
bash 0_download_updated_tables.sh # Takes a few min

# Update metadata table (outputs file configs/gwascat_metadata_merged.tsv)
python 1_create_gwascatalog_metadata_table.py

# Edit metadata table manually (!) to
# 1. Flag which studies should be imported in the "to_ingest" column (currently, only studies that are European)
# 2. Check total sample size, case count for case-control studies
# 3. Save as configs/gwas_metadata_curated.latest.tsv

# Copy updated metadata table to GCS
gsutil cp 'configs/gwas_metadata_curated.latest.tsv' gs://genetics-portal-dev-sumstats/unfiltered/metadata/gwas_metadata_curated.${version_date}.tsv
gsutil cp 'configs/gwas_metadata_curated.latest.tsv' gs://genetics-portal-dev-sumstats/unfiltered/metadata/
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas_${version_date}/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt

# Download the sumstats we need to GCS (those not already completed).
# Run steps from here:
tmux
bash 2_download_gwascatalog_to_gcs.sh $version_date

# Get list of input files on GCS
gsutil -m ls gs://genetics-portal-dev-raw/gwas_catalog/harmonised_${version_date}/\*.tsv.gz > configs/gwascatalog_inputs/gcs_input_paths.txt

# Create manifest file
# This requires input file configs/gwas_metadata_curated.latest.tsv
# Creates output file configs/gwascatalog.manifest.json
python 3_create_gwascatalog_manifest.py $version_date

# To test just 5 initially...
#cp configs/gwascatalog.manifest.json configs/gwascatalog.manifest.json.bak
#head -n 5 configs/gwascatalog.manifest.json.bak > configs/gwascatalog.manifest.json
#tail -n +6 configs/gwascatalog.manifest.json.bak > configs/gwascatalog.manifest.json

# Start cluster (see below)
# Then set region
gcloud config set dataproc/region europe-west1

# Submit jobs to cluster
# Use tmux first, since submitting many jobs can take a while
python run_all.py

# Check that its working as expected, then increase cluster number of workers
```

### QC

Check outputs and any errors
```
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas_${version_date}/*.parquet/_SUCCESS" > configs/gwascatalog_outputs/ingest_completed_paths.txt
#gsutil -m ls -l "gs://genetics-portal-dev-sumstats/logs/unfiltered/ingest/gwas_catalog/*.log" > configs/gwascatalog_outputs/ingest_completed_logfile_list.txt
gsutil cat -h gs://genetics-portal-dev-sumstats/logs/unfiltered/ingest/gwas_${version_date}/*.log/*.txt > configs/gwascatalog_outputs/ingest_logs_all.txt
```

Get a few lines from each input file where we don't find the output file present, to help identify why these GWAS failed. Looking through this file should help to determine, for each failed GWAS, what the cause might be. Check whether beta/OR are present, whether harmonised SNP IDs are present, whether the odds ratios are valid/sensible
```
# (Takes 20+ min for 500+ GWAS)
time python 4_check_completed_files.py > failed_file_ingests.txt
```

After going through the failed file ingests, you may want to annotate the file of curated metadata (configs/gwas_metadata_curated.latest.tsv) with any suitable notes, and upload that to GCS as above, so that these same GWAS aren't selected for ingestion in the future.

When everything is done, delete the raw sumstat files on GCS.
```
gsutil -m rm -r gs://genetics-portal-dev-raw/gwas_catalog/harmonised_${version_date}
```

### Starting a Dataproc cluster

```
# Start large server
# I have tested performance on different configurations, and the pipeline is mainly
# CPU-bound, but this interacts with spark minimums for workers, so it ends up seeming
# that an n2-standard configuration is best.
gcloud beta dataproc clusters create \
    js-ingest-gwascatalog \
    --region europe-west1 \
    --image-version=1.5-ubuntu18 \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n2-standard-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-secondary-workers=0 \
    --worker-machine-type=n2-standard-8 \
    --num-workers=4 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=20m

# Or run single-node
gcloud beta dataproc clusters create \
    js-ingest-gwascatalog \
    --region europe-west1 \
    --image-version=1.5-ubuntu18 \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=7,spark:spark.executor.instances=1 \
    --metadata 'CONDA_PACKAGES=scipy' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --master-machine-type=n2-standard-16 \
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
gcloud compute ssh js-ingest-gwascatalog-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-ingest-gwascatalog-m" http://js-ingest-gwascatalog-m:8088

# To update the number of workers
gcloud dataproc clusters update js-ingest-gwascatalog \
    --region=europe-west1 \
    --num-workers=4 \
    --num-secondary-workers=4
```

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc