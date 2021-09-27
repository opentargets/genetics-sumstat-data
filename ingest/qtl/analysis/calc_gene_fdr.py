#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import sys
import os
import argparse
import pandas as pd
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def main():

    # Args
    args = parse_args()

    df = pd.read_csv(args.minp)

    # Bonferroni correction per gene on number of tests done
    df['p_bonf'] = df['min_pval'] * df['num_tests']
    # Don't allow pvals > 1
    df['p_bonf'] =  df['p_bonf'].apply(lambda x: x if x <= 1 else 1)

    # Compute FDR q values
    df = (
        df
        .groupby(['study_id', 'bio_feature'])
        .apply(lambda x: get_bh_fdr(x))
    )

    # Ensure that no q value is larger than a q value that comes after it
    # in the p value ranking
    print("Making q-values non-descending")
    study = df['study_id'][len(df.index)-1]
    bio_feature = df['bio_feature'][len(df.index)-1]
    print(f'Study: {study}\tBiofeature: {bio_feature}')

    # This is an inefficient way to do it...
    fdr_col = df.columns.get_loc('qval')
    study_col = df.columns.get_loc('study_id')
    bio_feature_col = df.columns.get_loc('bio_feature')
    for i in reversed(range(0, len(df.index)-1)):
        if not (study == df.iloc[i, study_col] and bio_feature == df.iloc[i, bio_feature_col]):
            # different study/tissue, so restart qvalue checking
            study = df.iloc[i, study_col]
            bio_feature = df.iloc[i, bio_feature_col]
            print(f'Study: {study}\tBiofeature: {bio_feature}')
        else:
            if (df.iloc[i, fdr_col] > df.iloc[i+1, fdr_col]):
                df.iloc[i, fdr_col] = df.iloc[i+1, fdr_col]

    # Compute FDR threshold per dataset
    df.to_csv(args.out, index=False)

    return 0

def get_bh_fdr(df):
    df = df.sort_values(['p_bonf', 'min_pval'])
    df['qval'] = df['p_bonf'] * len(df.index) / range(1, len(df.index)+1)
    df['qval'] = df['qval'].apply(lambda x: x if x <= 1 else 1)

    # Ensure that no q value is larger than a q value that comes after it
    # in the p value ranking
    print(len(df.index)-1)
    #for i in reversed(range(0, len(df.index)-1)):
    #    if (df['qval'][i] > df['qval'][i+1]):
    #        df['qval'][i] = df['qval'][i+1]
    return(df)

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--minp', metavar="<file>", help=("Input file with min p value per gene"), type=str, required=True)
    parser.add_argument('--out', metavar="<file>", help=("Output file path"), type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
