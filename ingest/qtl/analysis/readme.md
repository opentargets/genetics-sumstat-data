Analyse patterns in QTL data
=======================

Here we explore patterns in the QTL data, such as number of QTLs at different p-value thresholds. This applies to sumstats that have already been ingested.


#### summary metrics about our QTLs

```
# Start large cluster
gcloud beta dataproc clusters create \
    js-qtl-analysis \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.executor.cores=30,spark:spark.executor.instances=1 \
    --master-machine-type=n1-highmem-32 \
    --master-boot-disk-size=2TB \
    --num-master-local-ssds=1 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=10m

# Submit job to calculate gene minp values across all QTL datasets
gcloud dataproc jobs submit pyspark \
    --cluster=js-qtl-analysis \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    calc_gene_min_pvalues.py

# Submit job to calculate gene minp values across all QTL datasets
gcloud dataproc jobs submit pyspark \
    --cluster=js-qtl-analysis \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    calc_gene_min_pvalues_old.py


# Compare number of genes at different pval/FDR thresholds
# calc_gene_fdr.py takes 10 min or so
gsutil cp gs://genetics-portal-dev-analysis/js29/molecular_trait/211202/min_pvals_per_gene.csv.gz .
python calc_gene_fdr.py --minp min_pvals_per_gene.csv.gz --out qvals_per_gene.csv.gz

gsutil cp gs://genetics-portal-dev-analysis/js29/molecular_trait/min_pvals_per_gene_old_2002.csv.gz .
python calc_gene_fdr.py --minp min_pvals_per_gene_old_2002.csv.gz --out qvals_per_gene_old.csv.gz

mkdir -p output
Rscript plot_qtl_numbers_old.R qvals_per_gene_old.csv.gz output_21.12/old
Rscript plot_qtl_numbers.R qvals_per_gene.csv.gz output_21.12/qtl_comparison

```

Monitor dataproc cluster:

```
gcloud compute ssh js-qtl-analysis-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-qtl-analysis-m" http://js-qtl-analysis-m:8088

```