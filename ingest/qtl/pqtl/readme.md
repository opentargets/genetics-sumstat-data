Ingest cis-pQTLs from harmonised sumstats
======================================================

Spark workflow to read protein QTL sumstats, mainly from a GWAS per protein, and filter to cis-window variants only.

pQTL sumstats have been harmonised by GWAS catalog, but we are ingesting them as QTLs prior to them being publicly available (EFOs not yet assigned). For QTLs we only want variants within a cis-window (1 Mb) around each protein's TSS.

### Setup
```
# Start & update conda env, if necessary
conda activate sumstats
conda install google-cloud-storage
```

### Pre-processing

Harmonised sumstats were obtained from a private EBI FTP server, since they were produced prior to EFO mapping and public harmonised data release.

```
# Copy each harmonised dataset
PROTEIN_PATH=gs://genetics-portal-ukbb-mr-eur/protein-gwas
gsutil -m rsync -r $PROTEIN_PATH/FOLKERSEN/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/FOLKERSEN/
gsutil -m rsync -r $PROTEIN_PATH/HILLARY/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/HILLARY/
gsutil -m rsync -r $PROTEIN_PATH/OLLI/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/OLLI/
gsutil -m rsync -r $PROTEIN_PATH/PIETZNER/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/PIETZNER/
gsutil -m rsync -r $PROTEIN_PATH/SCALLOP/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/SCALLOP/
gsutil -m rsync -r $PROTEIN_PATH/SUHRE/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/SUHRE/
gsutil -m rsync -r $PROTEIN_PATH/SUN/harmonised/ gs://genetics-portal-dev-raw/protein-gwas/harmonised/SUN/

# For running on Dataproc, mappings need to be on GCS
gsutil -m cp -r mappings/* gs://genetics-portal-dev-raw/protein-gwas/mappings

# For local testing
mkdir -p example_data
gsutil -m rsync -r gs://genetics-portal-dev-raw/protein-gwas/harmonised/FOLKERSEN/ example_data/FOLKERSEN/
gsutil cp gs://genetics-portal-dev-data/21.10/inputs/lut/homo_sapiens_core_104_38_genes.json.gz example_data/
```


### Start Dataproc cluster

```bash
# Single-node cluster
gcloud beta dataproc clusters create \
    js-pqtl-ingest \
    --image-version=2.0-ubuntu18 \
    --metadata 'CONDA_PACKAGES=pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=25g,spark:spark.executor.memory=60g,spark:spark.executor.cores=6,spark:spark.executor.instances=8 \
    --master-machine-type=n2-highmem-64 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m
#    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=8g,spark:spark.executor.memory=28g,spark:spark.executor.cores=4,spark:spark.executor.instances=3 \
#    --master-machine-type=n2-highmem-16 \

# Or start large cluster
gcloud beta dataproc clusters create \
    js-pqtl-ingest \
    --image-version=1.5-ubuntu18 \
    --metadata 'CONDA_PACKAGES=scipy pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=dataproc:efm.spark.shuffle=primary-worker \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.master=yarn \
    --master-machine-type=n2-highmem-8 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --num-secondary-workers=0 \
    --worker-machine-type=n2-highmem-8 \
    --num-workers=2 \
    --worker-boot-disk-size=1TB \
    --num-worker-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --max-idle=30m

# To update the number of workers
gcloud dataproc clusters update js-pqtl-ingest \
    --region=europe-west1 \
    --num-workers=4 \
    --num-secondary-workers=4

# To monitor
gcloud compute ssh js-pqtl-ingest-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-pqtl-ingest-m" http://js-pqtl-ingest-m:8088
```

### Run pQTL ingestion

```
# To specify datasets to ingest, copy relevant dataset lines from configs/dataset_meta.tsv
# to configs/dataset_manifest.tsv. The manifest specifies the datasets to ingest and other
# required parameters.
# In the initial ingest, only 4 of 7 datasets could be ingested in this standard way.
(head -n 1 configs/dataset_meta.tsv; cat configs/dataset_meta.tsv | grep 'FOLKERSEN|HILLARY|PIETZNER|SCALLOP') > configs/dataset_manifest.tsv
python run_all.py
```

Run ingestion for datasets whose format isn't quite standard.
```
(head -n 1 configs/dataset_meta.tsv; cat configs/dataset_meta.tsv | grep 'OLLI') > configs/dataset_manifest.tsv
python run_all.py --script scripts/process.olli.py 

(head -n 1 configs/dataset_meta.tsv; cat configs/dataset_meta.tsv | grep 'SUHRE') > configs/dataset_manifest.tsv
python run_all.py --script scripts/process.suhre.py 
```
We don't yet ingest sun using this pipeline - we can reuse the data from the old pipeline at ../pQTL_sun2018.


#### Extra
```
gcs_parquet_to_text.py -f gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_new/SUHRE_2017.parquet/bio_feature=UBERON_0001969/part-00000-55dac0bf-bd3f-4a21-bdf6-b59213f57747.c000.snappy.parquet -o suhre.part0.tsv
gcs_parquet_to_text.py -f gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_new/HILLARY_2019.parquet/bio_feature=UBERON_0001969/part-00000-81aafb2d-adca-4aa2-831b-466fde81aca5.c000.snappy.parquet -o hillary.part0.tsv
gcs_parquet_to_text.py -f gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_fixed/PIETZNER_2020.parquet/bio_feature=UBERON_0001969/part-00001-889573ba-b9af-48c0-aea4-a43ff99d500d.c000.snappy.parquet -o pietzner.part1.tsv
gcs_parquet_to_text.py -f gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_fixed/FOLKERSEN_2020.parquet/bio_feature=UBERON_0001969/part-00001-518ccdf7-9f64-4ae8-b79e-b798afadb230.c000.snappy.parquet -o folkersen.part1.tsv

gsutil rm -r gs://genetics-portal-dev-sumstats/unfiltered/molecular_trait_fixed

gsutil cp -r 

```


#### Check headers
Some of the harmonised file headers have different columns for data from the same study.
This screws up pyspark if we try to read in all files at once, since I think it assumes
a common format. Let's check the headers.
```
for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/OLLI/*.tsv`; do
  echo $f >> olli.fnames.tsv
done
for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/OLLI/*.tsv`; do
  gsutil cat $f | head -n 1 >> olli.headers.tsv
done
paste olli.fnames.tsv olli.headers.tsv > olli.file_headers.tsv

for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/HILLARY/*.tsv`; do
  echo $f >> HILLARY.fnames.tsv
done
for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/HILLARY/*.tsv`; do
  gsutil cat $f | head -n 1 >> HILLARY.headers.tsv
done
paste HILLARY.fnames.tsv HILLARY.headers.tsv > HILLARY.file_headers.tsv

for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/SUHRE/*.tsv`; do
  echo $f >> SUHRE.fnames.tsv
done
for f in `gsutil ls gs://genetics-portal-dev-raw/protein-gwas/harmonised/SUHRE/*.tsv`; do
  gsutil cat $f | head -n 1 >> SUHRE.headers.tsv
done
paste SUHRE.fnames.tsv SUHRE.headers.tsv > SUHRE.file_headers.tsv


```


```
gcloud beta dataproc clusters create \
    js-pqtl-fix \
    --image-version=2.0-ubuntu18 \
    --metadata 'CONDA_PACKAGES=pandas' \
    --initialization-actions gs://dataproc-initialization-actions/python/conda-install.sh \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=8g,spark:spark.executor.memory=18g,spark:spark.executor.cores=4,spark:spark.executor.instances=3 \
    --master-machine-type=n2-highmem-16 \
    --master-boot-disk-size=1TB \
    --num-master-local-ssds=0 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --max-idle=10m

gcloud dataproc jobs submit pyspark \
    --cluster=js-pqtl-fix \
    --region europe-west1 \
    fix_study_id.py


gcloud compute ssh js-pqtl-fix-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-pqtl-fix-m" http://js-pqtl-fix-m:8088

```

