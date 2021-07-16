#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy, Jeremy Schwartzentruber
#
import sys
import os
import json
import pandas as pd
import yaml
import pprint

def main():

    # Args
    out_manifest = 'configs/manifest.json'
    in_studies = pd.read_csv('configs/tabix_ftp_paths.tsv', sep='\t')
    # Currently we only use studies for whole gene expression
    in_studies = in_studies[in_studies.quant_method.isin(['ge', 'microarray'])]

    # Load analysis config file
    with open('configs/config.yaml', 'r') as in_h:
        config_dict = yaml.load(in_h)
    print('Config: \n' + pprint.pformat(config_dict, indent=2))
    
    with open(out_manifest, 'w') as out_h:
        for index, row in in_studies.iterrows():
            record = {}
            
            # Add fields
            record['study_id'] = row['study'].upper()
            record['qtl_group'] = row['qtl_group'].upper()
            record['quant_method'] = row['quant_method'].upper()
            
            # Get nominal input file name from path
            fname = row['ftp_path'].split('/')[-1]
            fname_base = fname.split('.')[0]
            record['in_nominal'] = config_dict['in_nominal'].format(fname)
            record['in_gene_meta'] = config_dict['in_gene_meta']
            record['in_biofeatures_map'] = config_dict['in_biofeatures_map']

            # Output file uses study_id as base name, and is partitioned on
            # bio_feature (tissue type and condition)
            record['out_parquet'] = config_dict['out_parquet'].format(record['study_id'], record['quant_method'], record['qtl_group'])
            
            # Write to manifest
            out_h.write(json.dumps(record) + '\n')

    return 0


if __name__ == '__main__':

    main()
