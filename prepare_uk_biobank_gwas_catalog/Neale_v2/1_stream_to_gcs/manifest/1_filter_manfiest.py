#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Filter the Neale V2 manifest to contain only required phenotypes
#

import sys
import pandas as pd

def main():

    # Args
    inf = 'Manifest_201807.tsv'
    outf = 'Manifest_201807.filtered.tsv'

    # Load
    df = pd.read_csv(inf, sep='\t')
    print('Total rows: ', df.shape[0])

    # Filter sexes
    df = df[df.Sex == 'both_sexes']
    print('Post-sex rows: ', df.shape[0])
    # Filter IRNT
    df = df[~pd.isnull(df['Phenotype Code'])]
    df = df[~(df['Phenotype Code'].str.endswith('_irnt'))]
    print('Post-irnt rows: ', df.shape[0])
    # Filter field codes that don't start with a numeric (ICD traits)
    df = df[df['Phenotype Code'].str.contains('^[0-9]')]
    print('Post-ICD rows: ', df.shape[0])

    # Write
    df.to_csv(outf, sep='\t', index=None)

    return 0

if __name__ == '__main__':

    main()
