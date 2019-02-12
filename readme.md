Process summary stats for Open Target Genetics
==============================================

Workflows for processing summary statistics file for Open Targets Genetics.

### Requirements when adding new datasets
- Alleles should be harmonised so that ref and alt alleles are on the forward strand and the orientation matches the Ensembl VCF: https://github.com/opentargets/sumstat_harmoniser
- Alt allele should always be the effect allele
- For case-control studies where OR are not reported, betas should be converted to log_odds. If association test was run using a linear model (e.g. BOLT-LMM, Hail) then the correct formula to calculate log odds is:
  ```
  * log_OR    = β / (μ * (1 - μ))
  * log_ORse  = se / (μ * (1 - μ))
  * where μ   = case fraction = (n_cases / (n_cases + n_controls))
  * OR        = exp(log_OR)
  * OR 95% CI = exp(log_OR ± 1.96 * log_ORse)
  * Citation: https://data.broadinstitute.org/alkesgroup/BOLT-LMM/#x1-5200010.2
  ```
- Chromosome must be one of `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 'X', 'Y', 'MT']`
- Rows should be filtered to only contain variants with sufficiently high number of minor allele counts (minorAC) to be confident in the association estimate. minorAC = 25. Steps:
  ```
  * maf_threshold = minorAC / (2 * n)
  * where n       = total sample size, for quantitive traits
                  = min(n_cases, n_controls), for case-control traits
  * filter all rows to remove where MAF < maf_threshold
  ```
- If pval == 0, set to minimum float64
- NaN and null should be represented as empty string ""
  - TODO future versions should use 'NA' instead of empty field
- TODO add INFO score filter ?
- TODO If MAF is not reported by study, should it be estimated from a reference?
- TODO maf should be changed to eaf


### Columns new
```
message spark_schema {
  required binary study_id (UTF8);
  optional binary phenotype_id (UTF8);
  optional binary biofeature (UTF8); # to include
  optional binary quant_id (UTF8); # remove
  optional binary group_id (UTF8); # remove
  optional binary gene_id (UTF8);
  optional binary chrom (UTF8);
  optional int32 pos;
  optional binary ref (UTF8);
  optional binary alt (UTF8);
  optional int32 tss_dist; # remove
  optional double beta;
  optional double se;
  optional double pval;
  optional int32 N;
  optional int32 N_cases; # to add
  optional double EAF;
  optional int32 MAC;
  optional int32 num_tests; # molecular qtl only
  optional boolean is_sentinal; # remove
  optional double info;
  optional boolean is_cc; # to add
}

* chrom (str): chromosome [not null]
* pos_b37 (pos): position [not null]
* ref_al (str): reference allele (non-effect allele) [not null]
* alt_al (str): alt allele (effect allele) [not null]
* beta (float): beta for quantitiative study, log_OR for case-control [not null]
* se (float): standard error of beta for quantitiative study, se of log_OR for case-control [not null]
* pval (float): p-value. If pval == 0, set to minimum float64 [not null]
* n_samples_variant_level (int): total sample size (variant level) [nullable]
* n_samples_study_level (int):   total sample size (study level) [not null if n_samples_variant_level is null]
* n_cases_variant_level (int): number of cases (variant level) [nullable]
* n_cases_study_level (int):   number of cases (study level) [not null if n_cases_variant_level is null]
* eaf (float): effect allele frequency [nullable]
* maf (float): minor allele frequency [nullable]
* info (float): imputation quality [nullable]
* is_cc (bool): 'True' if case-control, 'False' if quantitative study [not null]
```

### Columns old
```
* variant_id_b37 (str): 'chr_pos_ref_alt' for GRCh37
* chrom (str): chromosome [not null]
* pos_b37 (pos): position in GRCh37 [not null]
* ref_al (str): reference allele (non-effect allele) [not null]
* alt_al (str): alt allele (effect allele) [not null]
* beta (float): beta for quantitiative study, log_OR for case-control [not null]
* se (float): standard error of beta for quantitiative study, se of log_OR for case-control [not null]
* pval (float): p-value. If pval == 0, set to minimum float64 [not null]
* n_samples_variant_level (int): total sample size (variant level) [nullable]
* n_samples_study_level (int):   total sample size (study level) [not null if n_samples_variant_level is null]
* n_cases_variant_level (int): number of cases (variant level) [nullable]
* n_cases_study_level (int):   number of cases (study level) [not null if n_cases_variant_level is null]
* eaf (float): effect allele frequency [nullable]
* maf (float): minor allele frequency [nullable]
* info (float): imputation quality [nullable]
* is_cc (bool): 'True' if case-control, 'False' if quantitative study [not null]
```

### Proposed summary stat folder structure

https://docs.google.com/document/d/18splDAKSlboKCQdAcLogexE_Zd3odv6qlTSPoBUI4aQ/edit

```
gwas
  genome_wide
    study_id
      trait_code
        {chromosome}-{study_id}-{trait_id}.tsv.gz
  targeted
    immunochip
      study_id
        trait_code
          {chromosome}-{study_id}-{trait_id}.tsv.gz
    metabochip
      ...
    ...

molecular_qtl
  eqtl
    study_id
      tissue/cell_id # E.g. uberon or cell ontology ID
        biomarker_id # E.g. ensembl ID
          {chromosome}-{study_id}-{tissue_id}-{biomarker_id}.tsv.gz
        ...
  pqtl
    study_id
      tissue/cell_id # E.g. uberon or cell ontology ID
        biomarker_id # E.g. uniprot ID
          {chromosome}-{study_id}-{tissue_id}-{biomarker_id}.tsv.gz
        ...
  metabolites
    study_id
      tissue/cell_id # E.g. uberon or cell ontology ID
        biomarker_id # E.g. chebi ID
          {chromosome}-{study_id}-{tissue_id}-{biomarker_id}.tsv.gz
        ...
  cell_counts # E.g. Astle et al blood cell indicies
    study_id
      tissue/cell_id # E.g. uberon or cell ontology ID
        biomarker_id  # E.g. uberon or cell ontology ID
          {chromosome}-{study_id}-{tissue_id}-{biomarker_id}.tsv.gz
        ...

sequencing
  exome
    study_id
      trait_code
        variant_level
          {chromosome}-{study_id}-{trait_id}.tsv.gz
        gene_level
          {chromosome}-{study_id}-{trait_id}.tsv.gz
  wgs
    study_id
      trait_code
        variant_level
          {chromosome}-{study_id}-{trait_id}.tsv.gz
        gene_level
          {chromosome}-{study_id}-{trait_id}.tsv.gz
```
