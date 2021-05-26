Ingest Finngen sumstats
============================

Spark workflow to read, clean and transfrom summary stats from Finngen dataset.

#### 0. Remove old FinnGen version
Here we remove the old FinnGen version from all storage locations, including processed data
```
# Copy old ingested sumstats
gsutil -m cp -r 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/FINNGEN_R4*' 'gs://genetics-portal-dev-sumstats/finngen_R4/unfiltered/'

# Copy significant windows
gsutil -m cp -r 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/FINNGEN_R4*' 'gs://genetics-portal-dev-sumstats/finngen_R4/filtered/significant_window_2mb/'

# Copy p-val filtered sumstats
gsutil -m cp -r 'gs://genetics-portal-dev-sumstats/filtered/pvalue_0.05/gwas/FINNGEN_R4*' 'gs://genetics-portal-dev-sumstats/finngen_R4/filtered/pvalue_0.05/'

gsutil du gs://genetics-portal-dev-sumstats/finngen_R4/unfiltered/ > finngen_moved.du.txt
gsutil du gs://genetics-portal-dev-sumstats/finngen_R4/filtered/significant_window_2mb/ > finngen_moved.du.txt

# No need to move fine-mapping outputs, since they are already in their own folder, e.g.
# gs://genetics-portal-dev-staging/finemapping/finngen_210509

# Everything is copied - check that it's all there (if we need to keep it)
# Remove from original locations
gsutil -m rm -r 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/FINNGEN_R4*'
gsutil -m rm -r 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/FINNGEN_R4*'
gsutil -m rm -r 'gs://genetics-portal-dev-sumstats/filtered/pvalue_0.05/gwas/FINNGEN_R4*'

```

#### 1. Ingest sumstats

```
# Get list of input files on GCS (no need to download Finngen raw files to our own google storage)
# First test on subset of Finngen files (this line), then on full set (second line below)
gsutil -m ls 'gs://finngen-public-data-r5/summary_stats/finngen_R5_AB1_HIV*.gz' > configs/inputs/gcs_input_paths_finngen.txt
#gsutil -m ls 'gs://finngen-public-data-r5/summary_stats/finngen_R5*.gz' > configs/inputs/gcs_input_paths_finngen.txt

# Get list of existing output files
gsutil -m ls 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/FINNGEN_R5*/_SUCCESS' > configs/inputs/gcs_completed_paths.txt

# Get list of phenotypes
curl https://r5.finngen.fi/api/phenos | jq -r '.[]| @json' > configs/inputs/r5_finngen.json

# Create manifest file
python create_finngen_manifest.py

# Start cluster (see below)

# Submit jobs to cluster
python run_all.py

# Check that its working as expected, then increase cluster number of workers


# Check outputs and any errors
```

#### Starting a Dataproc cluster

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
            --master-machine-type=n1-highmem-4 \
            --master-boot-disk-size=1TB \
            --num-master-local-ssds=0 \
            --num-secondary-workers=0 \
            --worker-machine-type=n1-highmem-2 \
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
    --num-workers=30 \
    --project=open-targets-genetics-dev

```

Dataproc info: https://stackoverflow.com/questions/36506070/how-to-queue-new-jobs-when-running-spark-on-dataproc
Dataproc page: https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces
