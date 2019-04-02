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

    # Manifest files from Neale and SAIGE
    in_neale = 'configs/ukb_inputs/neale2_phenotypes.tsv'
    in_saige = 'configs/ukb_inputs/saige_phenotypes.tsv'

    # List of paths to summary stats on GCS
    # gsutil -m ls gs://genetics-portal-raw/uk_biobank_sumstats/harmonised_split_190318/\*.GRCh38.tsv.split000.gz > configs/ukb_inputs/gcs_input_paths.txt
    in_gs_path_list = 'configs/ukb_inputs/gcs_input_paths.txt'

    # List of completed datasets on GCS
    # gsutil ls "gs://genetics-portal-sumstats-b38/unfiltered/gwas/*/_SUCCESS" > configs/ukb_inputs/gcs_completed_paths.txt
    # Use `in_completed_path_list = None` if first run
    in_completed_path_list = 'configs/ukb_inputs/gcs_completed_paths.txt'

    # Input variant index on GCS
    gs_gnomad_path = 'gs://genetics-portal-data/variant-annotation/190129/variant-annotation.parquet'

    # Path to write the merged phenotypes file to use in study table later
    out_ukb_phenos = 'configs/ukb_outputs/ukb_phenotypes.tsv'

    # Path to write main manifest file
    out_manifest = 'configs/ukb.manifest.json'

    # Output directory for sumstats on GCS
    out_gs_path = 'gs://genetics-portal-sumstats-b38/unfiltered/gwas/'

    #
    # Load --------------------------------------------------------------------
    #

    # Load neale phenotpes
    neale = (
        pd.read_csv(in_neale, sep='\t')
        .drop(['PHESANT_transformation', 'variable_type', 'notes', 'source', 'n_missing'], axis=1)
        .rename(columns={
            'phenotype': 'code',
            'description': 'trait',
            'n_non_missing': 'n_total',
            'n_cases': 'temp_cases',
            'n_controls': 'temp_controls'
        })
    )
    # Calc actual n_cases as min of cases and controls
    neale['n_cases'] = neale[['temp_controls', 'temp_cases']].apply(min, axis=1)
    neale = neale.drop(['temp_controls', 'temp_cases'], axis=1)
    # Add prefix to code
    neale['code'] = 'NEALE2_' + neale['code']

    # Load saige phenotypes
    saige = (
        pd.read_csv(in_saige, sep='\t', dtype={'PheCode': 'str'})
        .drop(['url_sum', 'URL QQ Plot', 'URL-QQPlot',
               'URL Manhattan Plot', 'URL-ManhattanPlot',
               'Number of excluded controls'
               ], axis=1)
        .rename(columns={
            'PheCode': 'code',
            'Phenotype Description': 'trait',
            'Phenotype Category': 'trait_category',
            'Number of cases': 'n_cases',
            'Number of controls': 'n_controls'
        })
    )
    # Convert str to int
    saige['n_cases'] = saige['n_cases'].apply(lambda x: int(x.replace(',', '')))
    saige['n_controls'] = saige['n_controls'].apply(lambda x: int(x.replace(',', '')))
    # Convert . to _ in phecode
    saige['code'] = saige['code'].str.replace('.', '_')
    saige['code'] = 'SAIGE_' + saige['code']
    # Calc n_total
    saige['n_total'] = saige['n_cases'] + saige['n_controls']
    saige = saige.drop('n_controls', axis=1)

    # Get all input file paths from GCS
    # gcs_paths = load_gcs_paths(in_gs_path)
    # gcs = pd.DataFrame(gcs_paths, columns=['in_path'])
    gcs = pd.read_csv(in_gs_path_list, sep='\t', header=None, names=['in_path'])
    gcs['in_path'] = gcs['in_path'].str.replace('.split000.', '.split*.')

    # Extract code
    gcs['code'] = gcs['in_path'].apply(parse_code)
    # gcs.to_csv('temp.gcs.tsv', sep='\t', index=None)

    #
    # Merge phenotypes and paths together -------------------------------------
    #

    # Write phenotype file to use in study table later
    phenotypes = pd.concat([neale, saige], sort=False)
    merged = pd.merge(gcs, phenotypes, on='code', how='left')

    # Filter to remove uneeded studies
    uneeded_study_prefixes = [
        'NEALE2_20024_', # Job codes
        'NEALE2_22601_',  # Job codes
        'NEALE2_22617_',  # Job codes
        'NEALE2_22660_',  # Job codes
        'NEALE2_20079_',  # Day of week questionnaire
        'NEALE2_20118_',  # Home area
        'NEALE2_41248_',  # Hospital discharge destination 
        ]
    for code_prefix in uneeded_study_prefixes:
        to_remove = merged.code.str.startswith(code_prefix)
        merged = merged.loc[~to_remove, :]

    # Write for later
    os.makedirs(os.path.dirname(out_ukb_phenos), exist_ok=True)
    merged.to_csv(out_ukb_phenos, sep='\t', index=None)

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
        for in_record in merged.to_dict(orient='records'):
            out_record = {}

            # Create output path and check if it already exists
            out_record['out_parquet'] = out_gs_path + in_record['code'] + '.parquet'
            if out_record['out_parquet'] in completed:
                continue

            # Add fields
            out_record['in_tsv'] = in_record['in_path']
            out_record['in_af'] = gs_gnomad_path
            out_record['study_id'] = in_record['code']
            out_record['n_total'] = int(in_record['n_total'])
            try:
                out_record['n_cases'] = int(in_record['n_cases'])
            except ValueError:
                out_record['n_cases'] = None

            # Write
            out_h.write(json.dumps(out_record) + '\n')

    return 0

def parse_code(in_path):
    ''' Parses the phenotype code from the path name
    '''
    bn = os.path.basename(in_path)
    if bn.startswith('PheCode'):
        code = (
            bn.split('_SAIGE_Nov2017')[0]
            .replace('PheCode_', '')
            .replace('.', '_')

        )
        code = 'SAIGE_' + code
    elif '.neale2.' in bn:
        code = bn.split('.neale2.')[0]
        code = 'NEALE2_' + code
    
    return code

def load_gcs_paths(in_path):
    ''' Uses gsutil to glob file paths on gcs
    '''
    paths = []
    # Run gsutil
    proc = sp.Popen(
        'gsutil ls {0}'.format(in_path),
        shell=True,
        stdout=sp.PIPE
    )
    # Read stdin
    for line in proc.stdout:
        paths.append(line.decode().rstrip())
        
    return paths


if __name__ == '__main__':

    main()
