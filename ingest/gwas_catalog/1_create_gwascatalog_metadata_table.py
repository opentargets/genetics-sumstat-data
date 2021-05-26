#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Create a table containing GWAS Catalog summary statistic metadata
#


import sys
import os
import pandas as pd
import re

def main():

    # Args
    gcs_input_paths = 'configs/gwascatalog_inputs/gcs_input_paths.txt'
    #gcs_input_paths = 'configs/gwascatalog_inputs/local_input_paths.txt'
    in_study_info = 'configs/gwascatalog_inputs/gwascat_study_table.tsv'
    metadata = 'configs/gwascatalog_outputs/gwascat_metadata_original.tsv'
    # metadata_curation = metadata.replace('_original', '_curation')

    # Load list of available sumstat paths
    df = pd.read_csv(gcs_input_paths, sep='\t', header=None, names=['gcs_path'])

    # Load study info
    req_cols = {
        'STUDY ACCESSION': 'study_id',
        'PUBMEDID': 'pmid',
        'INITIAL SAMPLE SIZE': 'sample_info'
    }
    study_info = (
        pd.read_csv(in_study_info, sep='\t', header=0)
        .rename(columns=req_cols)
        .loc[:, req_cols.values()]
    )
    
    # Extract GCST ID from path
    df['study_id'] = df['gcs_path'].apply(parse_study_from_path)

    # Merge
    df = pd.merge(df, study_info, on='study_id', how='left')

    # Extract sample size stats
    n_counts = df['sample_info'].apply(
        extract_sample_sizes
    )
    df['n_cases'] = n_counts.apply(lambda x: x[0]).replace({0: None})
    df['n_controls'] = n_counts.apply(lambda x: x[1]).replace({0: None})
    df['n_quant'] = n_counts.apply(lambda x: x[2]).replace({0: None})

    # Write two copies
    os.makedirs(os.path.dirname(metadata), exist_ok=True)
    df.to_csv(metadata, sep='\t', index=None)
    # df.to_csv(metadata_curation, sep='\t', index=None)

    return 0


def extract_sample_sizes(s):
    ''' Extracts sample size info from GWAS Catalog field
    '''
    n_cases = 0
    n_controls = 0
    n_quant = 0
    for part in s.split(', '):
        # Extract sample size
        mtch = re.search('([0-9,]+)', part)
        if mtch:
            n = int(mtch.group(1).replace(',', ''))
            # Add to correct counter
            if 'cases' in part:
                n_cases += n
            elif 'controls' in part:
                n_controls += n
            else:
                n_quant += n
    # print([n_cases, n_controls, n_quant])
    return [n_cases, n_controls, n_quant]


def parse_study_from_path(path):
    ''' Returns the GWAS Catalog study ID
    '''
    stid = re.split(r'[-|_]', os.path.basename(path))[1]
    assert stid.startswith('GCST')
    return stid

if __name__ == '__main__':

    main()
