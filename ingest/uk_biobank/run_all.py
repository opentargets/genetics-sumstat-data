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
    in_manifest = 'configs/ukb.manifest.head.json'
    script = 'scripts/process.py'
    run_remote = True
    cluster_name = 'em-ingest-ukb'
    # https://stackoverflow.com/questions/32820087/spark-multiple-spark-submit-in-parallel
    starting_port = 4040

    # Run each job in the manifest
    for c, line in enumerate(open(in_manifest, 'r')):

        # if c != 0:
        #     continue

        manifest = json.loads(line)

        # Build script args
        args = [
            '--in_sumstats', manifest['in_tsv'],
            '--in_af', manifest['in_af'],
            '--out_parquet', manifest['out_parquet'],
            '--study_id', manifest['study_id'],
            '--n_total', str(manifest['n_total'])
        ]
        # Optional args
        if manifest['n_cases']:
            args.append('--n_cases')
            args.append(str(manifest['n_cases']))

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

        # Run command
        print('Running job {0}...'.format(c + 1))
        print(' '.join(cmd))
        sp.call(' '.join(cmd), shell=True)
        print('Complete\n')
        time.sleep(0.5)

    return 0


if __name__ == '__main__':

    main()
