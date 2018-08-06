#!/usr/bin/env snakemake
#
# Ed Mountjoy
#

import os
import sys
import pandas as pd

# Load config
fields,  = glob_wildcards('input_nealeukb_data/{field}.nealeUKB_20170915.assoc.clean.tsv.gz')

# Make list of which chromosomes are in each file
chroms_d = {}
for field in fields:
    inf = 'chrom_lists/{field}.chrom_list.txt'.format(field=field)
    # print(inf)
    # chroms_d[field] = [1, 2]
    try:
        chroms_d[field] = pd.read_csv(inf, header=None, sep='\t', nrows=None).iloc[:, 0].unique().tolist()
    except:
        chroms_d[field] = []

# Make list of targets
targets = []
for field, chroms in chroms_d.items():
    for chrom in chroms:
        targets.append(
            'output_nealeukb_data/NEALEUKB_{field}/UKB_{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.gz'.format(field=field, chr=chrom) )
        # break # Only make 1 target per study but snakemake will be tricked into making all.

rule all:
    input:
        targets

rule format_sumstat:
    """ Reformats the sumstat file
    """
    input:
        "input_nealeukb_data/{field}.nealeUKB_20170915.assoc.clean.tsv.gz"
    output:
        'output_nealeukb_data/NEALEUKB_{field}/UKB_{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.gz'
    shell:
        'python scripts/nealeukb.format_sumstats.py '
        '--inf {input} '
        '--outf {output} '
        '--chr {wildcards.chr}'

# rule make_bgzip:
#     ''' bgzip the file
#     '''
#     input:
#         'output_nealeukb_data/{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.gz'
#     output:
#         'output_nealeukb_data/{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.bgz'
#     shell:
#         'zcat < {input} | bgzip -c > {output}'
#
# rule make_tbi:
#     ''' make the index
#     '''
#     input:
#         'output_nealeukb_data/{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.bgz'
#     output:
#         'output_nealeukb_data/{field}/{chr}-NEALEUKB_{field}-UKB_{field}.tsv.bgz.tbi'
#     shell:
#         'tabix --sequence 2 --begin 3 --skip-lines 1 {input}'
