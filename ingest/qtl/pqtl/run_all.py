#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import subprocess as sp
import time
import pandas as pd
import yaml
import argparse

def main():

    in_args = parse_args()
    # Other settings
    in_manifest ='configs/dataset_manifest.tsv'
    #script = 'scripts/process.py' # now obsolete due to in_args.script
    run_remote = True
    cluster_name = 'js-pqtl-ingest'

    # Load config
    with open('configs/config.yaml') as config_input:
        config = yaml.load(config_input, Loader=yaml.FullLoader)

    # Load manifest
    manifest = pd.read_csv(in_manifest, sep='\t')

    # Run each job in the manifest
    for i, row in manifest.iterrows():
    
        # Get the job ID
        study_id = row['study_id']

        # Get the job args
        args = [
            '--study_id', row['study_id'],
            '--sample_size', str(row['sample_size']),
            '--bio_feature', row['bio_feature'],
            '--in_nominal', row['data_path'],
            '--in_filename_map', row['filename_map'],
            '--in_gene_meta', config['gene_meta'],
            '--in_af', config['variant_af'],
            '--out_parquet', config['out_dir'] + '/' + study_id + '.parquet',
        ]

        # Build command
        if run_remote == True:
            cmd = [
                'gcloud dataproc jobs submit pyspark',
                '--region {0}'.format(config['region']),
                '--cluster={0}'.format(cluster_name),
                #'--properties spark.submit.deployMode=cluster',
                #'--async',
                in_args.script,
                '--'
            ] + args
        else:
            cmd = [
                'python',
                in_args.script
            ] + args

            # Escape wildcards
            cmd = [arg.replace('*', '\\*') for arg in cmd]

        # Run command
        print('Running job {0}: {1}...'.format(i + 1, study_id))
        print(' '.join(cmd))
        sp.call(' '.join(cmd), shell=True)
        print('Complete\n')
        time.sleep(0.5)

    return 0


def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--script', metavar="<string>", help=('Path to script to run'), type=str, default='scripts/process.py')
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
