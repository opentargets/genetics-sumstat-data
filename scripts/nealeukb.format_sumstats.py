#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Ed Mountjoy
'''

import os
import sys
import argparse
import pandas as pd
from collections import OrderedDict

def main():

    # Parse args
    args = parse_args()

    # Load
    if args.verbose: print('Loading data...')
    dfs = []
    iter_csv = pd.read_csv(args.inf, sep='\t', header=0, nrows=None, iterator=True, chunksize=100000)
    for chunk in iter_csv:
        chunk.chrom = chunk.chrom.astype(str)
        chunk = chunk.loc[chunk.chrom == args.chr, :]
        dfs.append(chunk)
    data = pd.concat(dfs)

    # Rename columns
    req_cols = OrderedDict([
        ('snpid', 'variant_id_b37'),
        ('chrom', 'chrom'),
        ('pos', 'pos_b37'),
        ('other_allele', 'ref_al'),
        ('effect_allele', 'alt_al'),
        ('beta', 'beta'),
        ('se', 'se'),
        ('pval', 'pval'),
        ('n', 'n_samples_variant_level'),
        ('n_samples_study_level', 'n_samples_study_level'),
        ('n_cases_variant_level', 'n_cases_variant_level'),
        ('ncases', 'n_cases_study_level'),
        ('eaf', 'eaf'),
        ('maf', 'maf'),
        ('info', 'info'),
        ('is_cc', 'is_cc')
    ])
    data = ( data.rename(columns=req_cols)
                 .loc[:, req_cols.values()] )

    # Set whether study is case-control
    if ~pd.isnull(data.n_cases_study_level).any():
        data['is_cc'] = True
    else:
        data['is_cc'] = False

    # Set maf
    if args.verbose: print('Calculating maf...')
    data['maf'] = data.eaf.apply(af_to_maf)

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
        'is_cc': bool
        }
    # Convert non null column datatypes
    not_null = data.apply(lambda col: (~pd.isnull(col)).all(), axis=0)
    dtypes_dict_notnull = dict(
        (k, dtypes_dict[k]) for k in data.columns[not_null].tolist() )
    data = data.astype(dtypes_dict_notnull)

    # Assert chromosomes are permissible
    permissible_chrom = set([str(x) for x in range(1, 23)] + ['X', 'Y', 'MT'])
    assert set(data.chrom).issubset(permissible_chrom)

    # Convert to log_OR / log_ORse
    if data.is_cc.any():

        if args.verbose: print('Converting logOR and logORse...')

        # Get number of cases and controls
        n_cases = data.n_cases_study_level
        n_sample = data.n_samples_variant_level
        case_frac = n_cases / n_sample

        # Converts
        data['beta'] = data.beta / (case_frac * (1 - case_frac))
        data['se'] = data.se / (case_frac * (1 - case_frac))

    # Check that pval != 0
    if args.verbose: print('Fixing very low pvals...')
    data.loc[data.pval == 0, 'pval'] = (1 / sys.float_info.max)

    # Sort by position
    if args.verbose: print('Sorting and writing...')
    data = data.sort_values('pos_b37')

    # Write
    data.to_csv(args.outf, sep='\t', index=None, compression='gzip')

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
    parser.add_argument('--outf', metavar="<file>", help=('Output file pattern'), type=str, required=True)
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--chr', metavar="<str>", help=('Chromosome to process'), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
