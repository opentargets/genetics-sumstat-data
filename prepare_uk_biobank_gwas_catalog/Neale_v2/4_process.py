#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
import pyspark.sql
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

def main():

    # File args GCS
    # in_file_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/*.{type}.*.tsv.gz'
    in_file_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/raw/102*.{type}.*.tsv.gz'
    in_variant_index = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/variants.nealev2.parquet'
    in_phenotype = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/luts/phenotypes.both_sexes.filtered.tsv'
    out_pattern = 'gs://genetics-portal-raw/uk_biobank_sumstats/neale_v2/output/{pheno}.gwas.imputed_v3.both_sexes.tsv.gz'

    # File args local
    # in_file_pattern = 'example_data/*.{type}.*.head.tsv.gz'
    # in_variant_index = '2_make_variant_index/variants.nealev2.parquet'
    # in_phenotype = '1_stream_to_gcs/manifest/phenotypes.both_sexes.filtered.tsv'
    # out_pattern = 'example_out/{pheno}.gwas.imputed_v3.both_sexes.tsv.gz'

    # Make spark session
    global spark
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)

    #
    # Load and prepare sumstats ------------------------------------------------
    #

    # Load sumstats
    df_ord = load_sumstats(in_file_pattern.format(type='ordinal'))
    df_bin = load_sumstats(in_file_pattern.format(type='binary'))
    df_cont = load_sumstats(in_file_pattern.format(type='continuous_raw'))

    # Take union, adding missing columns
    df = union_all([df_cont, df_ord, df_bin])

    # Split variant ID
    parts = split(df.variant, ':')
    df = (
        df.withColumn('chr', parts.getItem(0))
          .withColumn('pos', parts.getItem(1).cast('long'))
          .withColumn('ref', parts.getItem(2))
          .withColumn('alt', parts.getItem(3))
    )

    # Make phenotype ID from the filename
    df = df.withColumn('phenotype', build_pheno_udf(input_file_name()))

    # Repartition
    df = df.repartitionByRange('chr', 'pos', 'ref', 'alt')

    #
    # Join additional columns --------------------------------------------------
    #

    # Add variant info
    variants = ( spark.read.parquet(in_variant_index)
                      .select('chr', 'pos', 'ref', 'alt', 'rsid', 'info') )
    df = df.join(variants, on=['chr', 'pos', 'ref', 'alt'], how='inner')

    # Load phenotype info
    pheno = spark.read.csv(
        in_phenotype,
        sep='\t',
        inferSchema=True,
        enforceSchema=False,
        header=True)

    # Remove unnecessary columns
    pheno = pheno.select(
        'phenotype',
        'description',
        'source',
        'n_non_missing',
        'n_missing',
        'n_controls',
        'n_cases' )

    # Broadcast join
    df = df.join(broadcast(pheno), on='phenotype', how='inner')

    #
    # Process ------------------------------------------------------------------
    #

    # Calc logOR and logORse
    case_frac = df.n_cases / (df.n_cases + df.n_controls)
    logOR = df.beta / (case_frac * (1 - case_frac))
    logORse = df.se / (case_frac * (1 - case_frac))
    df = ( df.withColumn('logOR', logOR)
             .withColumn('logORse', logORse) )

    # Calc Odds ratios
    df = ( df.withColumn('OR', exp(logOR))
             .withColumn('OR_lowerCI', exp(df.logOR - 1.96 * df.logORse))
             .withColumn('OR_upperCI', exp(df.logOR + 1.96 * df.logORse)) )

    # Calculcate allele frequency
    df = df.withColumn('AF', df.AC / (2 * df.n_complete_samples))

    # Rename columns
    df = ( df.withColumnRenamed('rsid', 'variant_id')
             .withColumnRenamed('pval', 'p-value')
             .withColumnRenamed('chr', 'chromosome')
             .withColumnRenamed('pos', 'base_pair_location')
             .withColumnRenamed('OR', 'odds_ratio')
             .withColumnRenamed('OR_lowerCI', 'ci_lower')
             .withColumnRenamed('OR_upperCI', 'ci_upper')
             .withColumnRenamed('beta', 'beta')
             .withColumnRenamed('se', 'standard_error')
             .withColumnRenamed('ref', 'other_allele')
             .withColumnRenamed('alt', 'effect_allele')
             .withColumnRenamed('AF', 'effect_allele_frequency')
    )

    #
    # Output into one file per phenotype ---------------------------------------
    #

    # Get hadoop handle
    global sc, hadoop, conf, fs
    sc = spark.sparkContext
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)

    # Persist df
    df = df.persist()

    # Get list of phenotypes
    phenotypes = ( df.select('phenotype')
                     .distinct()
                     .rdd.map(lambda r: r[0])
                     .collect() )

    # Process each phenotype separately
    for pheno in phenotypes:

        # Filter
        df_out = df.filter(df.phenotype == pheno)

        # Save as single file
        outname = out_pattern.format(pheno=pheno)
        dataframeToHDFSfile(
            dataframe=df_out,
            dst_file=outname,
            header=True,
            delimiter='\t',
            compression='gzip',
            nullVal='NA'
        )

    return 0

def build_pheno(filename):
    ''' Returns phenotype from filename
    '''
    return filename.split('/')[-1].split('.')[0]

# Convert to UDF
build_pheno_udf = udf(build_pheno, StringType())

def union_all(dfs):
    ''' Take union of all spark dataframes
    Params:
        dfs ([spk.df])
    Returns
        spk.df
    '''

    # Get list of all columns names, maintaining original order
    cols = dedup_list(
        reduce(lambda x, y: x + y,
               map(lambda z: z.columns, dfs)
    ))

    # All dfs must have the same set of columns
    for i, df in enumerate(dfs):
        for missing_col in set(cols) - set(df.columns):
            dfs[i] = dfs[i].withColumn(missing_col, lit(None))
        dfs[i] = dfs[i].select(cols)

    # Take union
    union = reduce(DataFrame.union, dfs)

    return union

def dedup_list(seq):
    ''' Remove duplicates from a list whilst maintaining order
    '''
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def load_sumstats(in_pattern):
    ''' Loads neale sumstats into spark df
    '''
    df = spark.read.csv(in_pattern,
                        sep='\t',
                        inferSchema=True,
                        enforceSchema=False,
                        header=True)
    return df


################################################################################
# Below is code required to write a DF as a single file
# https://github.com/Tagar/abalon/blob/master/abalon/spark/sparkutils.py
################################################################################


# ## Basic wrappers around certain hadoop.fs.FileSystem API calls:
# ## https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html
#
def hdfs_exists (file_path):
    '''
    Returns True if HDFS file exists

    :param file_path: file patch
    :return: boolean
    '''

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#exists(org.apache.hadoop.fs.Path)
    return fs.exists(hadoop.fs.Path(file_path))

def hdfs_drop (file_path, recursive=True):
    '''
    Drop HDFS file/dir

    :param file_path: HDFS file/directory path
    :param recursive: drop subdirectories too
    '''

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#delete(org.apache.hadoop.fs.Path,%20boolean)
    return fs.delete(hadoop.fs.Path(file_path), recursive)


def hdfs_rename (src_name, dst_name):
    '''
    Renames src file to dst file name

    :param src_name: source name
    :param dst_name: target name
    '''

    # https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/FileSystem.html#rename(org.apache.hadoop.fs.Path,%20org.apache.hadoop.fs.Path)
    return fs.rename(hadoop.fs.Path(src_name), hadoop.fs.Path(dst_name))


def hdfs_file_size (file_path):
    '''
    Returns file size of an HDFS file exists

    :param file_path: file patch
    :return: file size
    '''

    # See https://hadoop.apache.org/docs/r2.8.2/api/org/apache/hadoop/fs/ContentSummary.html
    #   - getLength()
    return fs.getContentSummary(hadoop.fs.Path(file_path)).getLength()


def HDFScopyMerge (src_dir, dst_file, overwrite=False, deleteSource=False):

    """
    copyMerge() merges files from an HDFS directory to an HDFS files.
    File names are sorted in alphabetical order for merge order.
    Inspired by https://hadoop.apache.org/docs/r2.7.1/api/src-html/org/apache/hadoop/fs/FileUtil.html#line.382

    :param src_dir: source directoy to get files from
    :param dst_file: destination file to merge file to
    :param overwrite: overwrite destination file if already exists? this would also overwrite temp file if exists
    :param deleteSource: drop source directory after merge is complete
    """

    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    # determine order of files in which they will be written:
    files.sort(key=lambda f: str(f))

    if overwrite and hdfs_exists(dst_file):
        hdfs_drop(dst_file)

    # use temp file for the duration of the merge operation
    dst_file_tmp = "{}.IN_PROGRESS.tmp".format(dst_file)

    # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
    out_stream = fs.create(hadoop.fs.Path(dst_file_tmp), overwrite)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for filename in files:

            in_stream = fs.open(filename)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        hdfs_drop(src_dir)

    try:
        hdfs_rename(dst_file_tmp, dst_file)
    except:
        hdfs_drop(dst_file_tmp)  # drop temp file if we can't rename it to target name
        raise


def HDFSwriteString (dst_file, content, overwrite=True, appendEOL=True, compression='none'):

    """
    Creates an HDFS file with given content.
    Notice this is usable only for small (metadata like) files.

    :param dst_file: destination HDFS file to write to
    :param content: string to be written to the file
    :param overwrite: overwrite target file?
    :param appendEOL: append new line character?
    :param compression: none, bzip2 or gzip
    """

    if appendEOL:
        content += "\n"

    if compression=='gzip':
        import zlib
        compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 25)
        content = compressor.compress(content.encode()) + compressor.flush()
    elif compression=='bzip2':
        import bz2
        compressor = bz2.BZ2Compressor()
        content = compressor.compress(content.encode()) + compressor.flush()

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    try:
        out_stream.write(bytearray(content))
    finally:
        out_stream.close()


def dataframeToHDFSfile (dataframe,
                         dst_file,
                         overwrite=False,
                         header=True,
                         delimiter=',',
                         quoteMode='MINIMAL',
                         quote='"',
                         escape='\\',
                         compression='none',
                         nullVal='NA'
                         ):

    """
    dataframeToHDFSfile() saves a dataframe as a delimited file.
    It is faster than using dataframe.coalesce(1).write.option('header', 'true').csv(dst_file)
    as it doesn't require dataframe to be repartitioned/coalesced before writing.
    dataframeToTextFile() uses copyMerge() with HDFS API to merge files.

    :param dataframe: source dataframe
    :param dst_file: destination file to merge file to
    :param overwrite: overwrite destination file if already exists?
    :param header: produce header record? Note: the the header record isn't written by Spark,
               but by this function instead to workaround having header records in each part file.
    :param delimiter: delimiter character
    :param quote: character - by default the quote character is ", but can be set to any character.
                  Delimiters inside quotes are ignored; set to '\0' to disable quoting
    :param escape: character - by default the escape character is \, but can be set to any character.
                  Escaped quote characters are ignored
    :param quoteMode: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html
    :param compression: compression codec to use when saving to file. This can be one of the known
                        shorten names (none, bzip2, gzip). lz4, snappy and deflate only supported with header=False

    """

    if not overwrite and hdfs_exists(dst_file):
        raise ValueError("Target file {} already exists and Overwrite is not requested".format(dst_file))

    dst_dir = dst_file + '.tmpdir'

    if header and compression not in ['none', 'bzip2', 'gzip']:
        raise ValueError("Header compression only supports 'gzip' and 'bzip2'")

    (dataframe
        .write
        .option('header', False)    # always save without header as if there are multiple partitions,
                                    # each datafile will have a header - not good.
        .option('delimiter', delimiter)
        # .option('quoteMode', quoteMode)
        .option('quote', quote)
        .option('escape', escape)
        .option('nullValue', nullVal)
        .option('compression', compression)
        .mode('overwrite')     # temp directory will always be overwritten
        .csv(dst_dir)
        )

    if header:
        # we will create a separate file with just a header record
        header_record = delimiter.join(dataframe.columns)
        header_filename = "{}/--00_header.csv".format(dst_dir)  # have to make sure header filename is 1st in
                                                                # alphabetical order
        HDFSwriteString(dst_file=header_filename, content=header_record, compression=compression)

    HDFScopyMerge(dst_dir, dst_file, overwrite, deleteSource=True)

if __name__ == '__main__':

    main()
