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
    script = 'process.py'
    run_remote = True
    cluster_name = 'js-cluster-gtex8-ingest'

    # Run each job in the manifest
    for c, line in enumerate(open(in_manifest, 'r')):

        manifest = json.loads(line)

        # Build script args
        args = [
            '--study_id', manifest['study_id'],
            '--qtl_group', manifest['qtl_group'],
            '--in_nominal', manifest['in_nominal'],
            '--in_varindex', manifest['in_varindex'],
            '--out_parquet', manifest['out_parquet'],
            '--out_log', manifest['out_log']
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
