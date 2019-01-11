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
    inf = 'phenotypes.both_sexes.tsv'
    outf = 'phenotypes.both_sexes.filtered.tsv'

    # Load
    df = pd.read_csv(inf, sep='\t')
    print('Total rows: ', df.shape[0])

    # Filter IRNT
    df = df[~pd.isnull(df['phenotype'])]
    df = df[~(df['phenotype'].str.endswith('_irnt'))]
    print('Post-irnt rows: ', df.shape[0])
    # Filter field codes that don't start with a numeric (ICD traits)
    df = df[df['phenotype'].str.contains('^[0-9]')]
    print('Post-ICD rows: ', df.shape[0])

    # Write
    df.to_csv(outf, sep='\t', index=None)

    return 0

if __name__ == '__main__':

    main()
