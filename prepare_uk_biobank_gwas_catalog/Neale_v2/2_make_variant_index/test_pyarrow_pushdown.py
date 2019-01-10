#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#



import pyarrow
import pyarrow.parquet as pq
import pandas as pd
from functools import partial
import timeit
from glob import glob
import os
import dask.dataframe as dd

def main():

    # Args
    inf = 'variants.nealev2.parquet'
    inf_part = 'variants.nealev2.partitioned.parquet'
    n_rep=3

    # Check pyarrow version
    print('Pyarrow version: ', pyarrow.__version__)
    assert(pyarrow.__version__ >= '0.11.0')

    # Define filter
    # filter = [
    #     ('chr', '==', '16'),
    #     ('pos', '>', 100000),
    #     ('pos', '<', 200000)
    # ]
    filter = [('chr', '==', '16')]

    #
    # Pyarrow ------------------------------------------------------------------
    #

    # # Load using predicate pushdown
    print('Pyarrow unpartitioned: ', load_sumstats_pyarrow(inf, filter).shape)
    print('Pyarrow partitioned: ', load_sumstats_pyarrow(inf_part, filter).shape)

    # func = partial(load_sumstats, inf, None)
    # t = timeit.Timer(func).timeit(number=n_rep)
    # print('Time taken per iteration ({0}): {1:.3f} secs\n'.format(n_rep, float(t)/n_rep))

    #
    # Fastparquet --------------------------------------------------------------
    #

    # Load using predicate pushdown
    print('Dask/fastparquet w filters: ', load_sumstats_fastparquet(inf, filter).shape)
    print('Dask/fastparquet wo filters: ', load_sumstats_fastparquet(inf, None).shape)

    return 0

def load_sumstats_fastparquet(in_pq, filters):
    ''' Load parquet using Dask/fastparquet
    '''

    df = dd.read_parquet(
        glob(os.path.join(in_pq, '*.parquet')),
        filters=filters,
        engine='fastparquet'
    ).compute()

    return df

def load_sumstats_pyarrow(in_pq, filters):
    ''' Load parquet using Pandas/pyarrow
    '''

    df = (
        pq.ParquetDataset(in_pq, filters=filters)
          .read()
          .to_pandas()
    )

    return df

if __name__ == '__main__':

    main()
