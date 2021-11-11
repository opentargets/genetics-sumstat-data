#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
import pandas as pd

def main():

    eqtl_metadata_in = 'eqtl_catalogue.tabix_ftp_paths.flt.tsv'
    mappings_out = 'eqtl_catalogue.mappings.json'
    mappings_out_tsv = 'eqtl_catalogue.mappings.tsv'

    eqtl_metadata = pd.read_table(eqtl_metadata_in, sep='\t')

    # Use the tissue label + condition as the label text, ignoring 'naive' condition
    eqtl_metadata['label'] = eqtl_metadata['tissue_label'] + ' ' + eqtl_metadata['condition_label'].str.replace('naive', '')

    # Replace underscores with spaces
    eqtl_metadata['label'] = eqtl_metadata['label'].str.replace('_', ' ')

    # Rename tissue_ontology_id to biofeature_code, and qtl_group to biofeature_string
    eqtl_metadata = eqtl_metadata.rename(columns={'tissue_ontology_id': 'biofeature_code', 'qtl_group': 'biofeature_string'})

    eqtl_metadata['original_source'] = 'eqtl_catalogue_v2'

    # Keep only desired columns
    eqtl_metadata = eqtl_metadata[['original_source', 'study', 'biofeature_string', 'biofeature_code', 'label', 'tissue_label', 'condition_label']]

    eqtl_metadata.to_json(mappings_out, orient='records', lines=True)
    eqtl_metadata.to_csv(mappings_out_tsv, sep='\t', index=False)


if __name__ == '__main__':

    main()



