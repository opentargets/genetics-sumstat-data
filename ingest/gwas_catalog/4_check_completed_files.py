#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import pandas as pd
import json
import subprocess as sp

def main():

    in_manifest = 'configs/gwascatalog.manifest.json'

    # For each manifest line, check if the expected output file is there, and
    # if not, then concatenate some lines from the input file so we can
    # diagnose the problem.
    for c, line in enumerate(open(in_manifest, 'r')):

        manifest = json.loads(line)

        # Check if file is present on google cloud storage GCS
        expected_file = manifest['out_parquet'] + '/_SUCCESS'
        cmd = 'gsutil ls {0}'.format(expected_file)
        out = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        #print('output: ' + out.stdout.decode('utf-8').strip())
        
        if out.stdout.decode('utf-8').strip() == expected_file:
            #print('File {0} is present'.format(manifest['out_parquet']))
            continue

        print('STUDY: {0}, {1}, {2}'.format(manifest['study_id'], manifest['in_tsv'], manifest['out_parquet']))
        cmd = 'gsutil cat {0} | gzip -d | head'.format(manifest['in_tsv'])
        out = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE)
        print(out.stdout.decode('utf-8').strip() + '\n')

    return 0


if __name__ == '__main__':

    main()
