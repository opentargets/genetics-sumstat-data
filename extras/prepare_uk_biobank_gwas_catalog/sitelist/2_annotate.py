#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import hail as hl
import sys
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():

    # Args (global)
    chain_file = 'gs://hail-common/references/grch37_to_grch38.over.chain.gz'
    inf = 'gs://genetics-portal-raw/uk_biobank_sumstats/variant_sitelist/ukbiobank_neale_saige_sitelist.190321.tsv'
    in_ensembl = 'gs://genetics-portal-raw/ensembl_grch37_r95/homo_sapiens-chr*.vcf.*.gz'
    out_parquet = 'gs://genetics-portal-raw/uk_biobank_sumstats/variant_sitelist/ukbiobank_neale_saige_sitelist.190321.annotated.parquet'

    # # Args (local)
    # chain_file = 'input_data/grch37_to_grch38.over.chain.gz'
    # inf = 'ukbiobank_neale_saige_sitelist.head100k.tsv'
    # in_ensembl = 'input_data/homo_sapiens-chr1.head.vcf'
    # out_parquet = 'ukbiobank_neale_saige_sitelist.annotated.parquet'

    #
    # Load sitelist ------------------------------------------------------------
    #

    # Load data
    ht = hl.import_table(
        inf,
        no_header=True,
        min_partitions=128,
        types={
            'f0':'str',
            'f1':'int32',
            'f2':'str',
            'f3':'str'}
    )

    # Rename columns
    ht = ht.rename({
        'f0': 'chrom_b37',
        'f1': 'pos_b37',
        'f2': 'ref',
        'f3': 'alt'
    })

    # Create locus and allele
    ht = ht.annotate(
        locus=hl.locus(ht.chrom_b37, ht.pos_b37, 'GRCh37'),
        alleles=hl.array([ht.ref, ht.alt])
    ).key_by('locus', 'alleles')

    #
    # Do liftover --------------------------------------------------------------
    #

    # Add chain file
    rg37 = hl.get_reference('GRCh37')
    rg38 = hl.get_reference('GRCh38')
    rg37.add_liftover(chain_file, rg38)

    # Liftover
    ht = ht.annotate(
        locus_GRCh38 = hl.liftover(ht.locus, 'GRCh38')
    )

    # Convert to spark
    df = (
        ht.to_spark()
          .withColumnRenamed('locus_GRCh38.contig', 'chrom_b38')
          .withColumnRenamed('locus_GRCh38.position', 'pos_b38')
          .drop('locus.contig', 'locus.position', 'alleles')
    )

    #
    # Annotate with rsids ------------------------------------------------------
    #

    # Load ensembl
    ensembl = load_ensembl_vcf(in_ensembl)

    # Join
    df = df.join(ensembl,
                on=['chrom_b37', 'pos_b37', 'ref', 'alt'],
                how='left')
    #
    # Write output -------------------------------------------------------------
    #

    # Write
    (
        df.select('chrom_b37', 'pos_b37', 'chrom_b38', 'pos_b38', 'ref', 'alt', 'rsid')
          .write.parquet(out_parquet, mode='overwrite')
    )

    return 0

def load_ensembl_vcf(inf):
    ''' Loads the Ensembl VCF into using Spark. Using import_vcf
        causes errors.
    '''
    # Load in spark
    import_schema = (
        StructType()
        .add('chrom_b37', StringType())
        .add('pos_b37', IntegerType())
        .add('rsid', StringType())
        .add('ref', StringType())
        .add('alts', StringType())
        .add('qual', StringType())
        .add('filter', StringType())
        .add('info', StringType()))
    df = pyspark.sql.SparkSession.builder.getOrCreate().read.csv(
        path=inf,
        sep='\t',
        schema=import_schema,
        header=False,
        comment='#')

    # Split alleles and explode
    df = (
        df.drop('qual', 'filter', 'info')
          .withColumn('alts', split(col('alts'), ','))
          .withColumn('alt', explode(col('alts')))
          .drop('alts')
          .orderBy('chrom_b37', 'pos_b37', 'ref', 'alt')
          .distinct()
    )

    return df

if __name__ == '__main__':

    main()
