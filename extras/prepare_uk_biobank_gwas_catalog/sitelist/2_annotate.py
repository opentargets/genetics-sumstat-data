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
    inf = 'gs://genetics-portal-raw/uk_biobank_sumstats/variant_sitelist/ukbiobank_neale_saige_sitelist.tsv'
    outf = 'gs://genetics-portal-raw/uk_biobank_sumstats/variant_sitelist/ukbiobank_neale_saige_sitelist.annotated.tsv.gz'
    in_ensembl = 'gs://genetics-portal-raw/ensembl_grch37_r95/homo_sapiens-chr*.vcf.gz'

    # # Args (local)
    # inf = 'ukbiobank_neale_saige_sitelist.tsv'
    # in_ensembl = 'input_data/homo_sapiens-chr1.head.vcf.gz'
    # outf = 'ukbiobank_neale_saige_sitelist.annotated.tsv.gz'

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

    # ht = ht.head(1000) # DEBUG

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
    # Annotate with RSIDs ------------------------------------------------------
    #

    # Load ensembl
    ensembl = load_ensembl_vcf(in_ensembl)
    # ensembl = ensembl.head(50000) # DEBUG

    # Annotate rsids
    ht = ht.annotate(rsid=ensembl[ht.key].rsid)

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

    #
    # Write output -------------------------------------------------------------
    #

    # Clean output
    ht = (
        ht.annotate(
            chrom_b38=ht.locus_GRCh38.contig,
            pos_b38=ht.locus_GRCh38.position)
         .key_by()
         .drop('locus', 'alleles', 'locus_GRCh38')
         .select('chrom_b37',
                 'pos_b37',
                 'chrom_b38',
                 'pos_b38',
                 'ref',
                 'alt',
                 'rsid')
    )

    # Write
    ht.export(outf)

    return 0

def load_ensembl_vcf(inf):
    ''' Loads the Ensembl VCF into a hail table using Spark. Using import_vcf
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

    # Make into a hail table
    ht = hl.Table.from_spark(df)
    ht = ht.annotate(
        locus=hl.locus(ht.chrom_b37, ht.pos_b37, 'GRCh37'),
        alleles=hl.array([ht.ref, ht.alt])
    ).key_by('locus', 'alleles')

    return ht

if __name__ == '__main__':

    main()
