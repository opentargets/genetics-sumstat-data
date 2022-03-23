#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import os
import pandas as pd
import json

def main():

    #
    # Args --------------------------------------------------------------------
    #

    study_prefix = 'FINNGEN_R6_'
    # Manifest files from Finngen release
    in_finngen = 'configs/inputs/r6_finngen.json'
    in_gs_path_list = 'configs/inputs/gcs_input_paths_finngen.txt'

    # List of completed datasets on GCS
    # gsutil ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/inputs/gcs_completed_paths.txt
    # Use `in_completed_path_list = None` if first run
    in_completed_path_list = 'configs/inputs/gcs_completed_paths.txt'

    # Path to write main manifest file
    out_manifest = 'configs/finngen.manifest.json'

    # Output directory for sumstats on GCS
    out_gs_path = 'gs://genetics-portal-dev-sumstats/unfiltered/gwas_220212/'

    keep_columns = [
        'code',
        'trait',
        'trait_category',
        'n_cases',
        'n_controls'
    ]

    finngen = (
        pd.read_json(path_or_buf=in_finngen, lines=True)
        .rename(
            columns={
                'phenocode': 'code',
                'phenostring': 'trait',
                'category': 'trait_category',
                'num_cases': 'n_cases',
                'num_controls': 'n_controls'
            }
        )
    )
    finngen = finngen[keep_columns]
    finngen['code'] = study_prefix + finngen['code']
    finngen['n_total'] = finngen['n_cases'] + finngen['n_controls']

    gcs = pd.read_csv(in_gs_path_list, sep='\t', header=None, names=['in_path'])
    gcs['code'] = gcs['in_path'].apply(parse_code, prefix=study_prefix, splitBy='finngen_R6_')

    merged = pd.merge(gcs, finngen, on='code')
    # merged.to_csv('merged.tsv', sep='\t', index=None)

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
        for in_record in merged.to_dict(orient='records'):
            out_record = {}

            # Create output path and check if it already exists
            out_record['out_parquet'] = out_gs_path + in_record['code'] + '.parquet'
            if out_record['out_parquet'] in completed:
                continue

            # Add fields
            out_record['in_tsv'] = in_record['in_path']
            out_record['study_id'] = in_record['code']
            out_record['n_total'] = int(in_record['n_total'])
            try:
                out_record['n_cases'] = int(in_record['n_cases'])
            except ValueError:
                out_record['n_cases'] = None

            # Write
            out_h.write(json.dumps(out_record) + '\n')

    return 0

def parse_code(in_path, prefix, splitBy):
    ''' Parses the phenotype code from the path name
    '''
    return prefix + in_path.split(splitBy)[-1].split('.')[0]

if __name__ == '__main__':
    main()
