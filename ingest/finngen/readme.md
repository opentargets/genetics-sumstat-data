Ingest Finngen sumstats
============================

Spark workflow to read, clean and transfrom summary stats from Finngen dataset.

## 0. Remove old FinnGen version
Here we remove the old FinnGen version from all storage locations, including processed data
```
# Copy old ingested sumstats
FINN_VER=FINNGEN_R5
gsutil -m cp -r "gs://genetics-portal-dev-sumstats/unfiltered/gwas/$FINN_VER*" "gs://genetics-portal-dev-sumstats/$FINN_VER/unfiltered/"

# Copy significant window filtered sumstats
gsutil -m cp -r "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/$FINN_VER*" "gs://genetics-portal-dev-sumstats/$FINN_VER/filtered/significant_window_2mb/"

gsutil du gs://genetics-portal-dev-sumstats/$FINN_VER/unfiltered/ | gzip > finngen_moved.du.txt.gz
gsutil du gs://genetics-portal-dev-sumstats/$FINN_VER/filtered/significant_window_2mb/ | gzip > finngen_moved.sigwind.du.txt.gz

# No need to move fine-mapping outputs, since they are already in their own folder, e.g.
# gs://genetics-portal-dev-staging/finemapping/finngen_210509

# Everything is copied - check that it's all there (if we need to keep it)
# Remove from original locations
gsutil -m rm -r "gs://genetics-portal-dev-sumstats/unfiltered/gwas/$FINN_VER*"
gsutil -m rm -r "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/$FINN_VER*"
```

## 1. Ingest sumstats

You can view the sumstats at:
https://console.cloud.google.com/storage/browser/finngen-public-data-r6/summary_stats

```
NEW_GWAS_DIR=gwas_220212
# Get list of input files on GCS (no need to download Finngen raw files to our own google storage)
# First test on subset of Finngen files (this line), then on full set (second line below)
gsutil -m ls 'gs://finngen-public-data-r6/summary_stats/finngen_R6_AB1_HIV*.gz' > configs/inputs/gcs_input_paths_finngen.txt
#gsutil -m ls 'gs://finngen-public-data-r6/summary_stats/finngen_R6*.gz' > configs/inputs/gcs_input_paths_finngen.txt

# Get list of existing output files
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/$NEW_GWAS_DIR/FINNGEN_R6*/_SUCCESS" > configs/inputs/gcs_completed_paths.txt

# Get list of phenotypes
curl https://r6.finngen.fi/api/phenos | jq -r '.[]| @json' > configs/inputs/r6_finngen.json

# Create manifest file. (First update the code to use newest version numbers.)
python create_finngen_manifest.py

# Start cluster (see below)

# Submit jobs to cluster
python run_all.py

# Check that its working as expected, then increase cluster number of workers


# Check outputs and any errors
gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/$NEW_GWAS_DIR/FINNGEN_R6*/_SUCCESS" > configs/inputs/gcs_completed_paths.txt
```

## 2. Remove old sumstats

When the new data have been fully integrated, you may want to delete the old version.

```
gsutil -m rm -r "gs://genetics-portal-dev-sumstats/$FINN_VER"
```

## Starting a Dataproc cluster

```
# Start large server
# Note that when running the pipeline on this cluster, it seems to have very low
# utilisation of the available CPUs, so probably needs optimising in some way.
gcloud beta dataproc clusters create \
            js-ingest-finngen \
            --image-version=preview \
            --metadata 'CONDA_PACKAGES=scipy' \
            --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
            --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
            --master-machine-type=n1-highmem-8 \
            --master-boot-disk-size=1TB \
            --num-master-local-ssds=0 \
            --num-secondary-workers=0 \
            --worker-machine-type=n1-highmem-8 \
            --num-workers=2 \
            --worker-boot-disk-size=100GB \
            --num-worker-local-ssds=1 \
            --region=europe-west1 \
            --zone=europe-west1-d \
            --initialization-action-timeout=20m \
            --max-idle=10m \
            --project=open-targets-genetics-dev

# To monitor
gcloud compute ssh js-ingest-finngen-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-ingest-finngen-m" http://js-ingest-finngen-m:8088

# To update the number of workers
gcloud dataproc clusters update js-ingest-finngen \
    --region=europe-west1 \
    --num-workers=10 \
    --project=open-targets-genetics-dev

```

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc
Dataproc page: https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces
