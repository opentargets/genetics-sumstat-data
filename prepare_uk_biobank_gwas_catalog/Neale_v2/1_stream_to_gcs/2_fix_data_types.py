#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# //genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/135.gwas.imputed_v3.both_sexes.tsv.bgz

import subprocess as sp
import os
import sys

def main():

    # Args
    in_pheno='manifest/phenotypes.both_sexes.filtered.tsv'

    # Iterare over manifest
    c = 0
    for name, dtype in yeild_name_type(in_pheno):

        c += 1

        print('Processing ({0}) {1}...'.format(c, name))

        old_name = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/{name}.gwas.imputed_v3.both_sexes.tsv.gz'.format(name=name)
        new_name = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/{name}.{dtype}.gwas.imputed_v3.both_sexes.tsv.gz'.format(name=name, dtype=dtype)

        cmd = 'gsutil mv {old} {new}'.format(old=old_name, new=new_name)

        # Run
        print(cmd)
        cp = sp.run(cmd, shell=True)

        print('Done')


    return 0

def yeild_name_type(manifest):
    ''' Reads manifest and yields the name and type of file
    Params:
        manifest (file): input Neale phenotype file
    Returns:
        (str, str): source and dest paths
    '''
    with open(manifest, 'r') as in_h:
        in_h.readline() # Skip header
        for line in in_h:
            parts = line.rstrip().split('\t')
            yield parts[0], parts[2]


if __name__ == '__main__':

    main()
