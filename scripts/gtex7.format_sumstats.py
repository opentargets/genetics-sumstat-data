#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Ed Mountjoy
'''

import os
import sys
import argparse
import pandas as pd
import gc
from collections import OrderedDict

def main():

    # Parse args
    args = parse_args()

    # Create tissue folder
    tissue_dir = os.path.join(args.outdir, args.tissue)
    if not os.path.exists(tissue_dir):
        os.mkdir(tissue_dir)

    # Load specific chrom
    if args.verbose: print('Loading data...')
    dfs = []
    iter_csv = pd.read_csv(args.inf, sep='\t', header=0, nrows=None, iterator=True, chunksize=1000000)
    for chunk in iter_csv:
        # Extract chrom info
        chunk.variant_id = chunk.variant_id.str.replace('_b37', '')
        chunk['chrom'], chunk['pos'], chunk['ref'], chunk['alt'] = chunk.variant_id.str.split('_').str
        chunk.chrom = chunk.chrom.astype(str)
        chunk = chunk.loc[chunk.chrom == args.chrom, :]
        if chunk.shape[0] > 0:
            dfs.append(chunk)
    data = pd.concat(dfs)

    # Clean gene name
    data['gene_clean'] = data.gene_id.apply(lambda x: x.split('.')[0])

    # Add sample size
    data['n_samples_study_level'] = args.n

    # Rename columns
    req_cols = OrderedDict([
        ('variant_id', 'variant_id_b37'),
        ('chrom', 'chrom'),
        ('pos', 'pos_b37'),
        ('ref', 'ref_al'),
        ('alt', 'alt_al'),
        ('slope', 'beta'),
        ('slope_se', 'se'),
        ('pval_nominal', 'pval'),
        ('n', 'n_samples_variant_level'),
        ('n_samples_study_level', 'n_samples_study_level'),
        ('n_cases_variant_level', 'n_cases_variant_level'),
        ('ncases', 'n_cases_study_level'),
        ('eaf', 'eaf'),
        ('maf', 'maf'),
        ('info', 'info'),
        ('is_cc', 'is_cc'),
        ('gene_clean', 'gene')
    ])
    data = ( data.rename(columns=req_cols)
                 .loc[:, req_cols.values()] )

    # Set whether study is case-control
    data['is_cc'] = False

    # Set data types. Note, null columns cannot be coerced to int
    if args.verbose: print('Setting dtypes...')
    dtypes_dict = {
        'variant_id_b37': str,
        'chrom': str,
        'pos_b37': int,
        'ref_al': str,
        'alt_al': str,
        'beta': float,
        'se': float,
        'pval': float,
        'n_samples_variant_level': int,
        'n_samples_study_level': int,
        'n_cases_variant_level': int,
        'n_cases_study_level': int,
        'eaf': float,
        'maf': float,
        'info': float,
        'is_cc': bool,
        'gene': str
        }
    # Convert non null column datatypes
    not_null = data.apply(lambda col: (~pd.isnull(col)).all(), axis=0)
    dtypes_dict_notnull = dict(
        (k, dtypes_dict[k]) for k in data.columns[not_null].tolist() )
    data = data.astype(dtypes_dict_notnull)

    # Assert chromosomes are permissible
    permissible_chrom = set([str(x) for x in range(1, 23)] + ['X', 'Y', 'MT'])
    assert set(data.chrom).issubset(permissible_chrom)

    # Check that pval != 0
    if args.verbose: print('Fixing very low pvals...')
    data.loc[data.pval == 0, 'pval'] = (1 / sys.float_info.max)

    # Group by gene
    if args.verbose: print('Writing outputs...')
    for name, grp in data.groupby('gene'):

        gene = name

        # Create gene dir
        gene_dir = os.path.join(tissue_dir, gene)
        if not os.path.exists(gene_dir):
            os.mkdir(gene_dir)

        # Sort grp
        grp = grp.sort_values('pos_b37')
        # Drop gene
        grp = grp.drop('gene', axis=1)

        # Write
        outname = '{chrom}-{study_id}-{tissue_id}-{gene_id}.tsv.gz'.format(
            chrom=args.chrom,
            study_id='GTEX7',
            tissue_id=args.tissue,
            gene_id=gene
        )
        outloc = os.path.join(gene_dir, outname)
        grp.to_csv(outloc, sep='\t', index=None, compression='gzip')

        # Deallocate memory (may be unnecessary)
        del name, grp, gene
        gc.collect()

    return 0

def af_to_maf(af):
    ''' Converts allele frequency to minor allele frequency
    '''
    af = float(af)
    if af > 0.5:
        return 1 - af
    else:
        return af

def parse_args():
    """ Load command line args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--inf', metavar="<file>", help=('Input file'), type=str, required=True)
    parser.add_argument('--outdir', metavar="<file>", help=('Output directory'), type=str, required=True)
    parser.add_argument('--tissue', metavar="<file>", help=('Tissue name'), type=str, required=True)
    parser.add_argument('--n', metavar="<file>", help=('Sample size'), type=int, required=False)
    parser.add_argument('--chrom', metavar="<file>", help=('Only process this chrom'), type=str, required=False)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
