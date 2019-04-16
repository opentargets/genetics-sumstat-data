#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import sys
import os
import time
import subprocess as sp

def main():

    # Args
    in_paths = 'gcs_input_paths.txt'
    completed_paths = 'gcs_completed_paths.txt'
    script = 'filter_by_merge.py'
    cluster_name = 'em-sumstatfilter'
    window_to_extract = 2e6
    gwas_p_threshold = 5e-8

    # Load set of completed paths
    completed = set([])
    with open(completed_paths, 'r') as in_h:
        for line in in_h:
            completed.add(os.path.dirname(line.rstrip()))

    # Run each job in the manifest
    c = 0
    for i, line in enumerate(open(in_paths, 'r')):

        # if i != 0:
        #     continue

        # Make path names
        in_path = os.path.dirname(line.rstrip())
        data_type = get_datatype(in_path)
        out_path = in_path.replace(
            '/unfiltered/',
            '/filtered/significant_window_2mb/'
        )

        # Skip completed
        if out_path in completed:
            print('Skipping {}'.format(out_path))
            continue

        # Build script args
        args = [
            '--in_sumstats', in_path,
            '--out_sumstats', out_path,
            '--window', str(int(window_to_extract)),
            '--pval', str(float(gwas_p_threshold)),
            '--data_type', data_type
        ]

        # Build command
        cmd = [
            'gcloud dataproc jobs submit pyspark',
            '--cluster={0}'.format(cluster_name),
            '--properties spark.submit.deployMode=cluster',
            # '--properties spark.submit.deployMode=cluster,spark.executor.memory=4G,spark.executor.cores=4',
            # '--properties spark.submit.deployMode=cluster,spark.executor.memory=4G',
            '--async',
            script,
            '--'
        ] + args
        
        # Run command
        print('Running job {0}...'.format(i + 1))
        print(' '.join(cmd))
        sp.call(' '.join(cmd), shell=True)
        print('Complete\n')
        time.sleep(0.5)

        # # Add counter for queued
        # c += 1
        # if c == 3:
        #     sys.exit()

    return 0


def get_datatype(s):
    ''' Parses datatype from the input path name
    '''
    path_type = os.path.basename(os.path.dirname(s))
    if path_type == 'gwas':
        data_type = 'gwas'
    elif path_type == 'molecular_trait':
        data_type = 'moltrait'
    else:
        sys.exit('Error: could not determine data type from path name')
    return data_type

if __name__ == '__main__':

    main()
