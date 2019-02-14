#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import pandas as pd

def main():

    # Args
    infs = [
        'input_data/50_raw.neale2.gwas.imputed_v3.both_sexes.tsv.gz',
        'input_data/PheCode_401_SAIGE_Nov2017_MACge20.tsv.gz'
    ]
    outf = 'ukbiobank_neale_saige_sitelist.tsv'

    # Load
    dfs = [
        pd.read_csv(inf,
                    sep='\t',
                    # nrows=10,
                    header=0,
                    usecols=['chromosome', 'base_pair_location', 'other_allele', 'effect_allele'],
                    dtype={'chromosome':str, 'base_pair_location':int, 'other_allele':str, 'effect_allele':str}
                    )
        for inf in infs
    ]
    df = pd.concat(dfs)

    # Sort, drop duplicates and write
    (
        df.sort_values(['chromosome', 'base_pair_location', 'other_allele', 'effect_allele'])
          .drop_duplicates()
          .to_csv(outf, sep='\t', index=None, header=None)
    )

    return 0

if __name__ == '__main__':

    main()
