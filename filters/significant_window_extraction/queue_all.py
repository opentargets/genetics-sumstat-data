#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy, Jeremy Schwartzentruber

import sys
import os
import time
import re
import subprocess as sp

def main():

    print_only = False

    # Args
    output_dir = sys.argv[1]
    in_paths = 'gcs_input_paths.txt'
    completed_paths = 'gcs_completed_paths.txt'
    script = 'filter_by_merge.py'
    cluster_name = 'js-sumstatfilter'
    window_to_extract = 2e6
    gwas_p_threshold = 5e-8

    # Load set of completed studies
    completed_studies = set([])
    with open(completed_paths, 'r') as in_h:
        for line in in_h:
            completed_studies.add(os.path.basename(os.path.dirname(line.rstrip())))

    # Run each job in the manifest
    c = 0
    jobnum = 0
    for i, line in enumerate(open(in_paths, 'r')):
        
        # if i != 0:
        #     continue

        # Make path names
        in_path = os.path.dirname(line.rstrip())
        study = os.path.basename(in_path)
        data_type = get_datatype(in_path)
        out_path = os.path.join(output_dir, data_type, study)

        # Skip completed
        if study in completed_studies:
            print('Skipping {}'.format(study))
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
            '--async',
            '--project=open-targets-genetics-dev',
            '--region=europe-west1',
            script,
            '--'
        ] + args
        
        # Run command
        jobnum = jobnum + 1
        print('Running job {0}...'.format(jobnum))
        print(' '.join(cmd))
        if not print_only:
            sp.call(' '.join(cmd), shell=True)
            print('Complete\n')
            time.sleep(0.5)

    return 0


def get_datatype(s):
    ''' Parses datatype from the input path name
    '''
    path = os.path.dirname(s)
    if re.search('molecular_trait', s):
        data_type = 'molecular_trait'
    elif re.search('gwas', s):
        data_type = 'gwas'
    else:
        sys.exit(f'Error: could not determine data type from path name: {s}')
    return data_type

if __name__ == '__main__':

    main()
