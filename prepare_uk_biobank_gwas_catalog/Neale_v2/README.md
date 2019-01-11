Neale lab version 2
===================

Scripts to prepare [Neale lab V2](http://www.nealelab.is/uk-biobank)  UK Biobank summary statistics for GWAS Catalog database. ICD traits will not be included, as these will be handles by SAIGE results.

### Step 1: Download data to GCS

Output dir: `gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw`

### Step 2: Process data for transfer to GWAS Catalog

Needs doing:
- Add/rename standard columns
  'variant_id' = variant ID
  'p-value' = p-value
  'chromosome' = chromosome
  'base_pair_location' = base pair location
  'odds_ratio' = odds ratio
  'ci_lower' = lower 95% confidence interval
  'ci_upper' = upper 95% confidence interval
  'beta' = beta
  'standard_error' = standard error
  'effect_allele' = effect allele
  'other_allele' = other allele
  'effect_allele_frequency' = effect allele frequency
- Add other columns
  - Phenotype information?
  - Sample size
  - Case numbers
  - info
  - Others?
- Missing set to NA

Requirements:
  - Spark >= 1.3.0 with hadoop=2.7 (hadoop 3 does not include copyMerge)
  - Pyspark
