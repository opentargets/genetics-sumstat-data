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

def main():

    #
    # Args --------------------------------------------------------------------
    #

    # Metadata
    in_metadata = 'configs/gwascatalog_outputs/gwascat_metadata_curation.tsv'

    # List of completed datasets on GCS
    # gsutil ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt
    # Use `in_completed_path_list = None` if first run
    in_completed_path_list = 'configs/gwascatalog_inputs/gcs_completed_paths.txt'

    # Input variant index on GCS
    gs_gnomad_path = 'gs://genetics-portal-data/variant-annotation/190129/variant-annotation.parquet'

    # Path to write main manifest file
    out_manifest = 'configs/gwascatalog.manifest.json'

    # Output directory for sumstats on GCS
    out_gs_path = 'gs://genetics-portal-sumstats-b38/unfiltered/gwas/'

    #
    # Load --------------------------------------------------------------------
    #

    metadata = pd.read_csv(in_metadata, sep='\t', header=0)
    
    # Restict to european only
    print('{} studies European ({:.1f}%)'.format(
        metadata['is_eur'].sum(), 100 * metadata['is_eur'].sum() / metadata.shape[0]))
    print('{} studies non-European ({:.1f}%)'.format(
        (~metadata['is_eur']).sum(), 100 * ((~metadata['is_eur']).sum()) / metadata.shape[0]))
    metadata = metadata.loc[metadata['is_eur'], :]

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
                completed.add(path)

    #
    # Create manifest ---------------------------------------------------------
    #

    with open(out_manifest, 'w') as out_h:
        for in_record in metadata.sample(frac=1).to_dict(orient='records'):
            out_record = {}

            # Create output path and check if it already exists
            out_record['out_parquet'] = out_gs_path + in_record['study_id'] + '.parquet'
            if out_record['out_parquet'] in completed:
                continue

            # Add fields
            out_record['in_tsv'] = in_record['gcs_path']
            out_record['in_af'] = gs_gnomad_path
            out_record['study_id'] = in_record['study_id']
            out_record['n_total'] = int(in_record['n_total'])
            try:
                out_record['n_cases'] = int(in_record['n_cases'])
            except ValueError:
                out_record['n_cases'] = None

            # Write
            out_h.write(json.dumps(out_record) + '\n')

    return 0

if __name__ == '__main__':

    main()
