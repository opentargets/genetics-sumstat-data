Process QTL datasets
==============================================

The backend needs a table that maps from each QTL dataset to the tissue ontology code, text label, etc. Here we build that table.

## Create mapping table for eQTL catalogue
```
# For eQTL catalogue, QTL metadata comes from here
# Currently we only use gene expression sumstats, not other QTL types
wget https://raw.githubusercontent.com/eQTL-Catalogue/eQTL-Catalogue-resources/master/tabix/tabix_ftp_paths.tsv -O eqtl_catalogue.tabix_ftp_paths.tsv
head -n 1 eqtl_catalogue.tabix_ftp_paths.tsv > eqtl_catalogue.tabix_ftp_paths.flt.tsv
cat eqtl_catalogue.tabix_ftp_paths.tsv | awk 'BEGIN {FS="\t"} $7 == "ge" || $7 == "microarray"' >> eqtl_catalogue.tabix_ftp_paths.flt.tsv

# Convert the table to json format with the relevant fields
python get_eqtl_catalogue_mappings.py
```

## Merge together mapping tables from separate sources
```
cat eqtl_catalogue.mappings.json \
    pqtl_sun2018.mappings.json \
    eqtlgen_2018.mappings.json \
    > biofeature_labels.json
```

## Upload to GCS
```
gsutil cp biofeature_labels.json gs://genetics-portal-dev-staging/lut/211111/biofeature_labels.json
```
