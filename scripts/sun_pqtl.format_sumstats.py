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
    data = pd.read_csv(args.inf, sep='\t', header=0)
    data.Allele1 = data.Allele1.str.upper()
    data.Allele2 = data.Allele2.str.upper()
    # Make key
    data['var_key'] = ['{0}_{1}_{2}_{3}'.format(row[0], row[1], *sorted([row[2], row[3]]))
                       for row in data.loc[:, ['chromosome', 'position', 'Allele1', 'Allele2']].values.tolist()]

    # Load ensembl vcf and make key
    vcf = pd.read_csv(args.vcf, sep='\t', header=0)
    vcf.columns = ['chrom', 'pos', 'rsid', 'ref', 'alt', 'X', 'Y', 'info']
    vcf['var_key'] = ['{0}_{1}_{2}_{3}'.format(row[0], row[1], *sorted([row[2], row[3]]))
                       for row in vcf.loc[:, ['chrom', 'pos', 'ref', 'alt']].values.tolist()]
    vcf = vcf.drop(['info'], axis=1)

    # Merge
    merge = pd.merge(data, vcf, on='var_key', how='inner')
    print('Warning: {0} variants are not found in Ensembl VCF'.format(data.shape[0] - merge.shape[0]))

    # Harmonise
    merge['hm_effect'] = merge['Effect']
    to_harmonise = (merge.Allele1 == merge.ref)
    merge.loc[to_harmonise, 'hm_effect'] = merge.loc[to_harmonise, 'hm_effect'] * -1

    # Calc pvalue
    merge['pval'] = merge['log(P)'].rpow(10)

    # Make SNPID
    merge['snpid'] = ['{0}_{1}_{2}_{3}'.format(row[0], row[1], row[2], row[3])
                       for row in merge.loc[:, ['chrom', 'pos', 'ref', 'alt']].values.tolist()]

    # Rename columns
    req_cols = OrderedDict([
        ('snpid', 'variant_id_b37'),
        ('chrom', 'chrom'),
        ('pos', 'pos_b37'),
        ('ref', 'ref_al'),
        ('alt', 'alt_al'),
        ('hm_effect', 'beta'),
        ('StdErr', 'se'),
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
    merge = ( merge.rename(columns=req_cols)
                   .loc[:, req_cols.values()] )

    # Set study info
    merge.is_cc = False
    merge.n_samples_study_level = 3301

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
    not_null = merge.apply(lambda col: (~pd.isnull(col)).all(), axis=0)
    dtypes_dict_notnull = dict(
        (k, dtypes_dict[k]) for k in merge.columns[not_null].tolist() )
    merge = merge.astype(dtypes_dict_notnull)

    # Assert chromosomes are permissible
    permissible_chrom = set([str(x) for x in range(1, 23)] + ['X', 'Y', 'MT'])
    assert set(merge.chrom).issubset(permissible_chrom)

    # Check that pval != 0
    if args.verbose: print('Fixing very low pvals...')
    merge.loc[merge.pval == 0, 'pval'] = (1 / sys.float_info.max)

    # Sort by position
    if args.verbose: print('Sorting and writing...')
    merge = merge.sort_values('pos_b37')

    # Write
    merge.to_csv(args.outf, sep='\t', index=None, compression='gzip')

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
    parser.add_argument('--vcf', metavar="<file>", help=('VCF to harmonise against'), type=str, required=True)
    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
