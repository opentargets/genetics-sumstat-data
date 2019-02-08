#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import subprocess as sp
import os
import sys

def main():

    # Args
    in_manifest='manifest/SAIGE_UKBiobank_URL_Nov2017_v3.tsv'
    out_gcs='gs://genetics-portal-raw/uk_biobank_sumstats/saige_nov2017/raw'

    # Iterare over manifest
    c = 0
    for src, dest in yeild_src_dest_paths(in_manifest):

        c += 1

        gcs_dest = '{gcs}/{dest}'.format(gcs=out_gcs, dest=dest)
        print('Processing ({0}) {1}...'.format(c, gcs_dest))

        # Skip if output exists
        # if gcs_exists(gcs_dest):
        #     print('Skipping')
        #     continue

        # Construct command
        cmd = [
            'wget -qO- ',
            src,
            '| gsutil cp -n - ',
            gcs_dest
        ]

        # Run
        cp = sp.run(' '.join(cmd), shell=True)
        if cp.returncode != 0:
            sys.exit('Error, exiting.')

        print('Done')


    return 0

def yeild_src_dest_paths(manifest):
    ''' Reads manifest and yields source and destination paths
    Params:
        manifest (file): input Neale manifest file
    Returns:
        (str, str): source and dest paths
    '''
    with open(manifest, 'r') as in_h:
        in_h.readline() # Skip header
        for line in in_h:
            parts = line.rstrip().split('\t')
            src = parts[6].split(' ')[1]
            dest = src.split('/')[-1]
            yield src, dest

def gcs_exists(path):
    ''' Check if GCS path exists using gsutils
    '''
    cmd = 'gsutil ls {0}'.format(path)
    with open(os.devnull, 'w') as fnull:
        cp = sp.run(cmd, shell=True, stdout=fnull, stderr=sp.STDOUT)

    if cp.returncode == 0:
        return True
    else:
        return False

if __name__ == '__main__':

    main()
