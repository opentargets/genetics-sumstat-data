#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import argparse
import gzip

def main():

    # Parse args
    args = parse_args()

    # Load rsid and chr:pos sets from gwas catalog assocs
    var_set = parse_sets(args.gwas)

    # Parse vcf
    with gzip.open(args.vcf, 'r') as in_h:
        with gzip.open(args.out, 'w') as out_h:
            for line in in_h:
                lined = line.decode()
                # Skip headers
                if lined.startswith('#'):
                    continue
                chrom, pos, _, _, _, _, _, _ = lined.rstrip().split('\t')
                chrom_pos = '{0}:{1}'.format(chrom, pos)
                if chrom_pos in var_set:
                    out_h.write(line)

    return 0

def parse_sets(in_gwas):
    '''
    Args:
        in_gwas (str): Sun et al sumstat file
    Returns:
        set(rsids and chrom:pos)
    '''
    var_set = set([])
    with gzip.open(in_gwas, 'rb') as in_h:
        header = in_h.readline().decode().rstrip().split('\t')
        for line in in_h:
            parts = line.decode().rstrip().split('\t')
            chrom = parts[header.index('chromosome')]
            pos = parts[header.index('position')]
            chrom_pos = '{chrom}:{pos}'.format(chrom=chrom, pos=int(pos))
            var_set.add(chrom_pos)
    return var_set

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--gwas', metavar="<file>", help=('GWAS Catalog input'), type=str, required=True)
    parser.add_argument('--vcf', metavar="<file>", help=("VCF input"), type=str, required=True)
    parser.add_argument('--out', metavar="<str>", help=("Output"), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
