
# Get paths from the metadata file for studies that we have selected to ingest
cat configs/gwas_metadata_curated.latest.tsv | awk 'BEGIN{FS="\t"} $9 == /1/' | cut -f 1 > configs/ftp_paths.to_download.txt

# Download everything to a local directory.
# (Make sure there's lots of space! Bank on about 500 Mb per study)
# May want to tmux to ensure connection doesn't get lost.
head -n 5 configs/ftp_paths.to_download.txt > configs/ftp_paths.test.txt
mkdir sumstats
cd sumstats

tmux
#wget --input-file=../configs/ftp_paths.test.txt --base=ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/
wget --no-clobber --input-file=../configs/ftp_paths.to_download.txt --base=ftp://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/

#gsutil -m ls -l gs://genetics-portal-dev-raw/gwas_catalog/harmonised_${version_date}/
version_date=`date +%y%m%d`
gsutil -m cp *.tsv.gz gs://genetics-portal-dev-raw/gwas_catalog/harmonised_${version_date}/
