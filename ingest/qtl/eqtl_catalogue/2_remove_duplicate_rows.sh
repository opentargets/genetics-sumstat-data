#!/bin/bash
NCORES=$1

# The eQTL catalogue has duplicate rows for different rsIDs for the same variant
# and we do not want to keep these
if [ ! -d "$DIRECTORY" ]; then
  mkdir sumstats_dedup
fi

for f in sumstats_raw/*.tsv.gz; do
  newf=`basename $f`
  echo "zcat $f | cut -f 19 --complement | uniq | gzip > sumstats_dedup/$newf" >> dedup_commands.txt
done

echo "Running in parallel with $NCORES jobs"

time cat dedup_commands.txt | parallel -j $NCORES

# Check how many rows present before and after removing duplicates
for f in sumstats_raw/*.tsv.gz; do
  fname=`basename $f`
  nrows=`zcat $f | wc -l`
  echo -e "$fname\t${nrows}" >> nrows.sumstats_raw.txt
done

for f in sumstats_dedup/*.tsv.gz; do
  fname=`basename $f`
  nrows=`zcat $f | wc -l`
  echo -e "$fname\t${nrows}" >> nrows.sumstats_dedup.txt
done

