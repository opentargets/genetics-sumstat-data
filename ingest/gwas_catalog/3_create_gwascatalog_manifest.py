#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# 
#


import sys
import os
import pandas as pd
import json
import subprocess as sp
import re

def main():

    #
    # Args --------------------------------------------------------------------
    #
    version_date = sys.argv[1]

    # Metadata
    in_metadata = 'configs/gwas_metadata_curated.latest.tsv'

    # Available harmonised input files
    input_path_list = "configs/gwascatalog_inputs/gcs_input_paths.txt"

    # List of completed datasets on GCS
    # gsutil ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt
    # Use `in_completed_path_list = None` if first run
    in_completed_path_list = 'configs/gwascatalog_inputs/gcs_completed_paths.txt'
    
    # Input variant index on GCS
    gs_gnomad_path = 'gs://genetics-portal-data/variant-annotation/190129/variant-annotation.parquet'

    # Path to write main manifest file
    out_manifest = 'configs/gwascatalog.manifest.json'

    # Path on GCS where raw harmonised studies are stored
    gcs_harmonised_root = f'gs://genetics-portal-dev-raw/gwas_catalog/harmonised_{version_date}/'

    # Output directory for sumstats on GCS
    #out_gs_path = 'gs://genetics-portal-sumstats-b38/unfiltered/gwas/'
    out_gs_path = f'gs://genetics-portal-dev-sumstats/unfiltered/gwas_{version_date}/'
    out_log_path = f'gs://genetics-portal-dev-sumstats/logs/unfiltered/ingest/gwas_{version_date}/'

    #
    # Load --------------------------------------------------------------------
    #
    input_paths = pd.read_csv(input_path_list, header=None, names=['gcs_path'])

    metadata = pd.read_csv(in_metadata, sep='\t', header=0)
    
    # Restict to european only
    print('{} studies to already ingested'.format(int(metadata['ingested'].sum())))
    print('{} studies to ingest'.format(int(metadata['to_ingest'].sum())))
    print('{} studies excluded'.format(int(len(metadata.index) - metadata['ingested'].sum() - metadata['to_ingest'].sum())))
    metadata = metadata.loc[metadata['to_ingest'] > 0, :]

    # Validate input
    nulls = pd.isnull(metadata[['n_cases', 'n_controls', 'n_quant']])
    assert (nulls['n_cases'] == nulls['n_controls']).all(), 'Rows have n_cases but no n_controls or vice versa'
    assert (nulls['n_cases'] != nulls['n_quant']).all(), 'Rows have n_quant and ( n_cases or n_controls)'
    assert (~metadata['study_id'].duplicated(keep=False)).all(), 'There are duplicate study IDs in input'

    # Calculate n_total
    metadata['n_total'] = metadata.loc[:, ['n_cases', 'n_controls', 'n_quant']].sum(axis=1)
    
    #
    # Load set of completed datasets that should be skipped -------------------
    #   
    completed = set([])
    if in_completed_path_list:
        with open(in_completed_path_list, 'r') as in_h:
            for line in in_h:
                path = os.path.dirname(line.rstrip())
                study = parse_study_from_path(path)
                completed.add(study)

    #
    # Create manifest ---------------------------------------------------------
    #
    num_to_do = 0
    num_completed = 0
    with open(out_manifest, 'w') as out_h:
        for in_record in metadata.sample(frac=1).to_dict(orient='records'):
            out_record = {}
            study_id = in_record['study_id']

            # Create output path and check if it already exists
            out_record['out_parquet'] = out_gs_path + study_id + '.parquet'
            #if out_record['out_parquet'] in completed:
            #    num_completed = num_completed + 1
            #    continue
            if in_record['study_id'] in completed:
                num_completed = num_completed + 1
                continue
            
            # Add fields
            sumstat_filename = os.path.basename(in_record['gwascat_path'])
            out_record['in_tsv'] = gcs_harmonised_root + sumstat_filename
            
            # Only add to manifest if file exists
            if (not out_record['in_tsv'] in input_paths['gcs_path'].values):
                print('Study {}: File {} not found on GCS. Skipping.'.format(in_record['study_id'], out_record['in_tsv']))
                continue

            out_record['in_af'] = gs_gnomad_path
            out_record['study_id'] = study_id
            out_record['n_total'] = int(in_record['n_total'])
            try:
                out_record['n_cases'] = int(in_record['n_cases'])
            except ValueError:
                out_record['n_cases'] = None
            out_record['log'] = out_log_path + study_id + ".log"

            # Write
            out_h.write(json.dumps(out_record) + '\n')
            num_to_do = num_to_do + 1
    print('{} studies written to manifest ({} already completed)'.format(num_to_do, num_completed))

    return 0

def parse_study_from_path(path):
    ''' Returns the GWAS Catalog study ID
    '''
    #print(path)
    m = re.search(r'/\w+.parquet', path)[0]
    stid = re.split(r'/|\.', m)[1]
    #assert stid.startswith('GCST')
    return stid

if __name__ == '__main__':

    main()
