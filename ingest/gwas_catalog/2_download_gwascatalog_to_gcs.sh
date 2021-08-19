# Need to improve this process - we should ideally get a list of GWAS and
# the harmonised file locations from GWAS catalog, and then only download
# the files that we select for import. (E.g. we might only use files with
# an ancestry we can currently process.)

# Get list of latest GWAS catalog harmonised sumstats. Grep to ensure that
# the file name includes "GCST", which removes the many files from Suhre
# that don't have unique GCTS IDs. (Took >2 hrs last time I ran it)
#time lftp -c "open ftp://ftp.ebi.ac.uk && find pub/databases/gwas/summary_statistics/" | grep "GCST[^/]*.h.tsv.gz" > configs/gwascatalog_inputs/ftp_paths.txt
curl -s http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/harmonised_list.txt > configs/gwascatalog_inputs/ftp_paths.txt

# Get a list of GWAS catalog studies that have already been processed.
time gsutil -m ls "gs://genetics-portal-dev-sumstats/unfiltered/gwas/*/_SUCCESS" > configs/gwascatalog_inputs/gcs_completed_paths.txt

# Extract the GCST IDs from the completed paths
cat configs/gwascatalog_inputs/gcs_completed_paths.txt | perl -ne 'if ($_ =~ /(GCST.*)\.parquet/) {print $1."\n"}' > configs/gwascatalog_inputs/gcs_completed_ids.txt

# Filter the ftp paths to only have lines not already completed
cat configs/gwascatalog_inputs/ftp_paths.txt | grep -v -Ff configs/gwascatalog_inputs/gcs_completed_ids.txt > configs/gwascatalog_inputs/ftp_paths.to_do.txt

# Download everything to a local directory.
# (Make sure there's lots of space! Bank on about 500 Mb per study)
# May want to tmux to ensure connection doesn't get lost.
head -n 5 configs/gwascatalog_inputs/ftp_paths.to_do.txt > configs/gwascatalog_inputs/ftp_paths.test.txt
mkdir sumstats
cd sumstats
#wget --input-file=../configs/gwascatalog_inputs/ftp_paths.test.txt --base=ftp://ftp.ebi.ac.uk/
wget --no-clobber --input-file=../configs/gwascatalog_inputs/ftp_paths.to_do.txt --base=ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/

# In the latest ingest of GWAS catalog summary stats, I manually joined the local
# sumstat file paths with the GWAS catalog metadata, and then selected only GWAS
# with 95%+ European samples. I only upload these to Google storage.
# In future we should make the process more automatic to first identify the new
# studies to add, and then only download these. GWAS catalog will hopefully
# provide a file of all studies with harmonised sumstats and their locations.
ls * > ../configs/gwascatalog_inputs/local_input_paths.txt

# Delete downloaded files that aren't EUR, so don't need to go to GCS.
cat ../configs/gwascatalog_inputs/local_input_paths.txt | grep -v -Ff ../configs/gwascatalog_inputs/local_input_paths_EUR.txt > ../configs/gwascatalog_inputs/local_input_paths.to_delete.txt
while read f; do
  rm $f
done < ../configs/gwascatalog_inputs/local_input_paths.to_delete.txt

#gsutil -m ls -l gs://genetics-portal-dev-raw/gwas_catalog/harmonised_201019/
#gsutil -m cp *.tsv.gz gs://genetics-portal-dev-raw/gwas_catalog/harmonised_210313/
