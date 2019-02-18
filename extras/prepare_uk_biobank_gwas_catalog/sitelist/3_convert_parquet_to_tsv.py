#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import pandas as pd
import sys

def main():

    inf = sys.argv[1]
    outf = sys.argv[2]

    # Require pandas version>=0.24 to allow null ints
    dtypes = {
        'chrom_b37': str,
        'pos_b37': 'Int64',
        'chrom_b38': str,
        'pos_b38': 'Int64',
        'ref': str,
        'alt': str,
        'rsid': str,
    }

    # Load
    df = (
        pd.read_parquet(inf, engine='pyarrow')
          .astype(dtype=dtypes)
    )

    # Strip chr from GRCh38 contig names
    df['chrom_b38'] = df.chrom_b38.str.lstrip('chr')

    # Replace None string with nan
    df['chrom_b38'] = df.chrom_b38.replace(to_replace=[None, 'None'], value='NA')
    df['rsid'] = df.rsid.replace(to_replace=[None, 'None'], value='NA')

    # Sort
    df = df.sort_values(['chrom_b37', 'pos_b37', 'ref', 'alt'])

    # Write
    df.to_csv(outf, sep='\t', index=None, na_rep='NA')

    return 0

if __name__ == '__main__':

    main()
