#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import json

def main():

    # Args
    out_manifest ='configs/manifest.json'
    in_studies = [
        'Alasoo_2018',
        # 'BLUEPRINT',
        # 'CEDAR',
        'Fairfax_2012',
        'Fairfax_2014',
        'GENCORD',
        'GEUVADIS',
        'HipSci',
        'Naranbhai_2015',
        'Nedelec_2016',
        'Quach_2016',
        'Schwartzentruber_2018',
        'TwinsUK',
        'van_de_Bunt_2015'
    ]
    
    with open(out_manifest, 'w') as out_h:
        for study in in_studies:
            
            record = {}

            # Add fields
            record['study_id'] = study.upper()
            record['in_nominal'] = "gs://genetics-portal-raw/eqtl_db_v1/split/{}/*/*.nominal.sorted.txt.split*.gz".format(study)
            record['in_varinfo'] = "gs://genetics-portal-raw/eqtl_db_v1/split/{}/*/*.variant_information.txt.split*.gz".format(study)
            record['in_gene_meta'] = "gs://genetics-portal-raw/eqtl_db_v1/raw/*_gene_metadata.txt"
            record['in_biofeatures_map'] = "gs://genetics-portal-data/lut/biofeature_lut_190328.json"
            record['out_parquet'] = "gs://genetics-portal-sumstats-b38/unfiltered/molecular_trait/{}.parquet".format(record['study_id'])

            # Write to manifest
            out_h.write(json.dumps(record) + '\n')

    return 0


if __name__ == '__main__':

    main()
