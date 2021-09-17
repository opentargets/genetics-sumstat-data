#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import subprocess as sp
import json
import time

def main():

    # Args
    in_manifest ='configs/manifest.json'
    #in_manifest ='configs/manifest.1.json'
    script = 'scripts/process.py'
    run_remote = True
    cluster_name = 'em-cluster-eqtldb-ingest'

    # Run each job in the manifest
    for c, line in enumerate(open(in_manifest, 'r')):

        # if c == 0:
        #     continue

        manifest = json.loads(line)

        # Build script args
        args = [
            '--study_id', manifest['study_id'],
            '--qtl_group', manifest['qtl_group'],
            '--quant_method', manifest['quant_method'],
            '--in_nominal', manifest['in_nominal'],
            '--in_gene_meta', manifest['in_gene_meta'],
            #'--in_study_meta', manifest['in_study_meta'],
            '--in_biofeatures_map', manifest['in_biofeatures_map'],
            '--out_parquet', manifest['out_parquet']
        ]

        # Build command
        if run_remote == True:
            cmd = [
                'gcloud dataproc jobs submit pyspark',
                '--cluster={0}'.format(cluster_name),
                '--properties spark.submit.deployMode=cluster',
                '--async',
                script,
                '--'
            ] + args
        else:
            cmd = [
                'python',
                script
            ] + args

            # Escape wildcards
            cmd = [arg.replace('*', '\\*') for arg in cmd]

        # Run command
        print('Running job {0}...'.format(c + 1))
        print(' '.join(cmd))
        sp.call(' '.join(cmd), shell=True)
        print('Complete\n')
        time.sleep(0.5)

    return 0


if __name__ == '__main__':

    main()
