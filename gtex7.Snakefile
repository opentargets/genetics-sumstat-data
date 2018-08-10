#!/usr/bin/env snakemake
#
# Ed Mountjoy
#

import os
import sys
import pandas as pd
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider

# Get name of fields
tissues,  = glob_wildcards('input_gtex7_data/{tissue}.allpairs.txt.gz')

# Load sample size dict
ss_df = pd.read_csv('configs/gtex7_sample_sizes.tsv', sep='\t', header=None)
ss_dict = dict(zip(ss_df.loc[:, 0], ss_df.loc[:, 1]))

# Load tissue code dict
tc_df = pd.read_csv('configs/gtex7_tissue_codes.tsv', sep='\t', header=0)
tc_dict = dict(zip(tc_df.prefix, tc_df.uberon_code))

# chroms = [str(x) for x in range(1, 23)] + ['X']
chroms = ['1']

rule all:
    input:
        expand('logs/gtex7.{chrom}.{tissue}.done.txt', tissue=tissues, chrom=chroms)

rule format_sumstat:
    """ Reformats the sumstat file
    """
    input:
        'input_gtex7_data/{tissue}.allpairs.txt.gz'
    params:
        outdir='output_gtex7_data',
        n=lambda wildcards: ss_dict[wildcards.tissue],
        tissue_code=lambda wildcards: tc_dict[wildcards.tissue]
    output:
        'logs/gtex7.{chrom}.{tissue}.done.txt'
    # resources:
    #     mem_gb=40
    shell:
        'python scripts/gtex7.format_sumstats.py '
        '--inf {input} '
        '--outdir {params.outdir} '
        '--n {params.n} '
        '--chrom {wildcards.chrom} '
        '--tissue {params.tissue_code} && touch {output}'
