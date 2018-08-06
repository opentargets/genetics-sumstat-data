#!/usr/bin/env snakemake
#
# Ed Mountjoy
#

import os
import sys
import pandas as pd
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider

# Get name of fields
fields, pref  = glob_wildcards('input_sun2018_data/{field}/{pref}_meta_final_v1.tsv.gz')
fields = list(set(fields))

# Load field to uniprot dict
manifest = 'input_sun2018_data/001_SOMALOGIC_GWAS_protein_info.csv'
uniprot_df = pd.read_csv(manifest, sep=',', header=0)
uniprot_df = uniprot_df.loc[:, ['SOMAMER_ID', 'UniProt']].dropna()
uniprot_df.UniProt = uniprot_df.UniProt.str.replace(',', '_')
field2uniprot = dict(zip(uniprot_df.SOMAMER_ID, uniprot_df.UniProt))
uniprot2field = dict(zip(uniprot_df.UniProt, uniprot_df.SOMAMER_ID))

# Make list of targets
targets = []
for field in field2uniprot:
    for chrom in range(1, 23):
        targets.append(
            'output_sun2018_data/SUN2018/UBERON_0001969/UNIPROT_{uniprot}/{chr}-SUN2018-UBERON_0001969-UNIPROT_{uniprot}.tsv.gz'.format(uniprot=field2uniprot[field], chr=chrom) )

rule all:
    input:
        targets

rule format_sumstat:
    """ Reformats the sumstat file
    """
    input:
        sumstats=lambda wildcards: 'input_sun2018_data/{field}/{field}_chrom_{chr}_meta_final_v1.tsv.gz'.format(
            field=uniprot2field[wildcards.uniprot],
            chr=wildcards.chr),
        vcf='temp/{chr}.SUN2018.Ensembl.vcf.gz'
    output:
        'output_sun2018_data/SUN2018/UBERON_0001969/UNIPROT_{uniprot}/{chr}-SUN2018-UBERON_0001969-UNIPROT_{uniprot}.tsv.gz'
    shell:
        'python scripts/sun_pqtl.format_sumstats.py '
        '--inf {input.sumstats} '
        '--outf {output} '
        '--vcf {input.vcf}'

rule extract_vcf:
    input:
        vcf='temp/Homo_sapiens.grch37.vcf.gz',
        sumstat='input_sun2018_data/A1CF.12423.38.3/A1CF.12423.38.3_chrom_{chr}_meta_final_v1.tsv.gz'
    output:
        'temp/{chr}.SUN2018.Ensembl.vcf.gz'
    shell:
        'pypy3 scripts/sun_pqtl.extract_from_vcf.py '
        '--gwas {input.sumstat} '
        '--vcf {input.vcf} '
        '--out {output}'

rule get_ensembl_variation_grch37:
    ''' Download all Ensembl variation data
    '''
    input:
        FTPRemoteProvider().remote('ftp://ftp.ensembl.org/pub/grch37/update/variation/vcf/homo_sapiens/Homo_sapiens.vcf.gz')
    output:
        'temp/Homo_sapiens.grch37.vcf.gz'
    shell:
        'cp {input} {output}'
