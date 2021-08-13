#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
import sys
import os
import json
import pandas as pd
import yaml
import pprint

def main():

    tissues = [
        'Adipose_Subcutaneous',
        'Adipose_Visceral_Omentum',
        'Adrenal_Gland',
        'Artery_Aorta',
        'Artery_Coronary',
        'Artery_Tibial',
        'Brain_Amygdala',
        'Brain_Anterior_cingulate_cortex_BA24',
        'Brain_Caudate_basal_ganglia',
        'Brain_Cerebellar_Hemisphere',
        'Brain_Cerebellum',
        'Brain_Cortex',
        'Brain_Frontal_Cortex_BA9',
        'Brain_Hippocampus',
        'Brain_Hypothalamus',
        'Brain_Nucleus_accumbens_basal_ganglia',
        'Brain_Putamen_basal_ganglia',
        'Brain_Spinal_cord_cervical_c-1',
        'Brain_Substantia_nigra',
        'Breast_Mammary_Tissue',
        'Cells_Cultured_fibroblasts',
        'Cells_EBV-transformed_lymphocytes',
        'Colon_Sigmoid',
        'Colon_Transverse',
        'Esophagus_Gastroesophageal_Junction',
        'Esophagus_Mucosa',
        'Esophagus_Muscularis',
        'Heart_Atrial_Appendage',
        'Heart_Left_Ventricle',
        'Kidney_Cortex',
        'Liver',
        'Lung',
        'Minor_Salivary_Gland',
        'Muscle_Skeletal',
        'Nerve_Tibial',
        'Ovary',
        'Pancreas',
        'Pituitary',
        'Prostate',
        'Skin_Not_Sun_Exposed_Suprapubic',
        'Skin_Sun_Exposed_Lower_leg',
        'Small_Intestine_Terminal_Ileum',
        'Spleen',
        'Stomach',
        'Testis',
        'Thyroid',
        'Uterus',
        'Vagina',
        'Whole_Blood'
    ]
    # Args
    out_manifest = 'configs/manifest.json'

    # Load analysis config file
    with open('configs/config.yaml', 'r') as in_h:
        config_dict = yaml.load(in_h, Loader=yaml.FullLoader)
    print('Config: \n' + pprint.pformat(config_dict, indent=2))
    
    with open(out_manifest, 'w') as out_h:
        for tissueName in tissues:
            record = {}

            # Add fields
            # Currently we only ingest GTEx sQTLs here
            record['study_id'] = 'GTEx-sQTL'
            record['qtl_group'] = tissueName
            
            # Make the expected file path
            #GTEx_data_path = 'gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_sQTL_all_associations'
            GTEx_data_path = 'gs://genetics-portal-dev-raw/gtex_v8/GTEx_Analysis_v8_EUR_sQTL_all_associations'
            record['in_nominal'] = f'{GTEx_data_path}/{tissueName}.v8.EUR.sqtl_allpairs.chr*.parquet'

            record['in_varindex'] = config_dict['variant_index']

            # Output file uses study_id as base name, and is partitioned on
            # bio_feature (tissue type and condition)
            record['out_parquet'] = config_dict['out_parquet'].format(record['study_id'], record['qtl_group'])
            record['out_log'] = config_dict['out_log'].format(record['study_id'], record['qtl_group'])
            
            # Write to manifest
            out_h.write(json.dumps(record) + '\n')

    return 0


if __name__ == '__main__':

    main()
