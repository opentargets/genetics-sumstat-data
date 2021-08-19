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
import pickle
import json

def main():

    # Args
    gcs_input_paths = 'configs/gwascatalog_inputs/ftp_paths.txt'
    in_study_info = 'configs/gwascatalog_inputs/gwascat_study_table.tsv'
    completed_paths = 'configs/gwascatalog_inputs/gcs_completed_paths.txt'
    prev_metadata_input = 'configs/gwas_metadata_curated.latest.previous.tsv'
    metadata_out = 'configs/gwascat_metadata_merged.tsv'

    # Load list of available sumstat paths
    df = pd.read_csv(gcs_input_paths, sep='\t', header=None, names=['gwascat_path'])

    # Load study info
    req_cols = {
        'STUDY ACCESSION': 'study_id',
        'PUBMEDID': 'pmid',
        'INITIAL SAMPLE SIZE': 'sample_info'
    }
    study_info = (
        pd.read_csv(in_study_info, sep='\t', header=0, dtype={'PUBMEDID':str})
        .rename(columns=req_cols)
        .loc[:, req_cols.values()]
    )

    # Extract GCST ID from path
    df['study_id'] = df['gwascat_path'].apply(parse_study_from_ftp_path)

    # Remove studies that don't have an expected GCST ID
    df = df[df['study_id'].str.startswith('GCST')]

    # Merge
    df = pd.merge(df, study_info, on='study_id', how='left')
    num_rows_orig = len(df.index)
    df.dropna(inplace=True)
    num_rows_dropped = num_rows_orig - len(df.index)
    print(f'Dropped {num_rows_dropped} rows not present in GWAS catalog metadata')

    # Extract sample size stats
    n_counts = df['sample_info'].apply(
        extract_sample_sizes
    )
    df['n_cases'] = n_counts.apply(lambda x: x[0]).replace({0: None})
    df['n_controls'] = n_counts.apply(lambda x: x[1]).replace({0: None})
    df['n_quant'] = n_counts.apply(lambda x: x[2]).replace({0: None})

    # Remove studies with duplicated IDs
    # (So far this applies to Suhre et al, with 1124 studies having the same ID)
    id_list = df['study_id'].tolist()
    id_dict = {id:id_list.count(id) for id in id_list}
    num_rows_orig = len(df.index)
    df = df[ [id_dict[id] <= 1 for id in df['study_id']]]
    num_rows_dropped = num_rows_orig - len(df.index)
    print(f'Dropped {num_rows_dropped} rows with duplicate study IDs')

    with open('duplicate_study_ids.json', 'w') as f:
        dup_dict = {id: count for id, count in id_dict.items() if count > 1}
        json.dump(dup_dict, f)

    prev_metadata = pd.read_csv(prev_metadata_input, sep='\t')
    prev_metadata.drop(columns=['gwascat_path', 'pmid', 'sample_info', 'n_cases', 'n_controls', 'n_quant', 'ingested'], inplace=True)

    # Merge with previous metadata table
    merged = pd.merge(
        df,
        prev_metadata,
        on='study_id',
        how='left'
    )

    # Add in lines from previous metadata that weren't merged
    missing_studies = set(prev_metadata['study_id']).difference(merged['study_id'])
    merged = merged.append(prev_metadata[prev_metadata['study_id'].isin(missing_studies)],
                           ignore_index=True)

    # Add a column indicating whether the study is already ingested
    completed = pd.read_csv(completed_paths, sep='\t', header=None, names=['path'])
    completed['study_id'] = completed['path'].apply(find_study_id)
    completed['ingested'] = '1'
    
    merged = pd.merge(
        merged,
        completed[['study_id', 'ingested']],
        on='study_id',
        how='left'
    )
    merged['to_ingest'] = ''
    
    # Write merged metadata
    os.makedirs(os.path.dirname(metadata_out), exist_ok=True)
    merged.to_csv(metadata_out, sep='\t', index=None)

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


def find_study_id(text):
    ''' Returns the first matching GCST ID from a string
    '''
    m = re.search('(GCST[0-9]+)', text)
    if m:
        return(m.group(1))
    return("")

def parse_study_from_ftp_path(path):
    ''' Returns the GWAS Catalog study ID from a GWAS cat FTP path
    '''
    stid = re.split(r'[/]', path)[2]
    if not stid.startswith('GCST'):
        print(path)
    #assert stid.startswith('GCST')
    return stid

if __name__ == '__main__':

    main()
