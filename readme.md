Process summary stats for Open Target Genetics
==============================================

Workflows for processing summary statistics file for Open Targets Genetics.

Unfiltered (full) data:
- GWAS: `gs://genetics-portal-sumstats-b38/unfiltered/gwas`
- Molecular trait: `gs://genetics-portal-sumstats-b38/unfiltered/molecular_trait`

Filtered to p < 0.05:
- GWAS: `gs://genetics-portal-sumstats-b38/filtered/pvalue_0.05/gwas`
- Molecular trait: `gs://genetics-portal-sumstats-b38/filtered/pvalue_0.05/molecular_trait`

Filtered to keep regions within 2Mb of a "significant" association:
- GWAS: `gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/gwas`
- Molecular trait: `gs://genetics-portal-sumstats-b38/filtered/significant_window_2mb/molecular_trait`

All datasets are in Apache Parquet format. These can be read in python using Spark or Pandas via pyarrow or fastparquet.

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
- Chromosome must be one of `['1', '2', '3', '4', '5', '6', '7', '8', '9', 1'0', 1'1', 1'2', 1'3', 1'4', 1'5', 1'6', 1'7', 1'8', 1'9', 2'0', 2'1', 2'2', 'X', 'Y', 'MT']`
- Rows should be filtered to only contain variants with sufficiently high number of minor allele counts. As of May 2019, we are using MAC>=10 for GWAS studies and MAC>=5 for molecular traits.
- If pval == 0, set to minimum float64

**Important note**. The fine-mapping and coloc pipelines currently use Dask to read the parquet files in python. We should continue to do this until [pyarrow has implemented row group filtering (predicate pushdown)](https://issues.apache.org/jira/browse/ARROW-1796), expected v0.14.0. In the mean time, all parquet files written in Spark should have the following option enabled: `pyspark.sql.SparkSession.builder.config("parquet.enable.summary-metadata", "true").getOrCreate()`

### Columns

- `type`: The study type. GWAS studies must be `gwas`. Molecular trait studies can take other values, e.g. `eqtl`, `pqtl`.
- `study_id`: A unique identifier for the study, should match the root parquet dataset name.
- `phenotype_id`: Null for type == gwas. ID for the measured phenotype for molecular traits. E.g. Illumina probe, or Ensembl gene/transcript ID.
- `bio_feature`: Null for type == gwas. An ID for the tissue measured in the molecular QTL. Ideally, this should be from an ontology such as CLO or UBERON.
- `gene_id`: Null for type == gwas. Ensembl gene ID otherwise.
- `chrom`: Chromosome of the variant.
- `pos`: Position of the variant in GRCh38 build.
- `ref`: Reference allele.
- `alt`: Alternative allele. All effects should be with respect to the alt allele.
- `beta`: The effect size wrt to `alt`. LogOR for binary traits.
- `se`: The standard error for `beta`. LogORse for binary traits.
- `pval`: Association p-value
- `n_total`: The total number of samples including cases and controls
- `n_cases`: Null for quantitative traits. The number of cases.
- `eaf`: The effect (`alt`) allele frequency. This could be estimated from a reference panel if not known.
- `mac`: The minor allele count in all samples.
- `mac_cases`: The minor allele count in cases.
- `num_tests`: The number of variants tested for each gene in the molecular QTL analysis.
- `info`: The imputation quality information.
- `is_cc`: Whether the study is case-control or not.

#### Schema
```
message spark_schema {
  required binary type (UTF8);
  required binary study_id (UTF8);
  optional binary phenotype_id (UTF8);
  optional binary bio_feature (UTF8);
  optional binary gene_id (UTF8);
  optional binary chrom (UTF8);
  optional int32 pos;
  optional binary ref (UTF8);
  optional binary alt (UTF8);
  optional double beta;
  optional double se;
  optional double pval;
  optional int32 n_total;
  optional int32 n_cases;
  optional double eaf;
  optional double mac;
  optional double mac_cases;
  optional int32 num_tests; # molecular qtl only
  optional double info;
  optional boolean is_cc;
}
```
