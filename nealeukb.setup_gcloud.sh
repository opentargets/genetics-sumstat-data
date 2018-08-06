#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p software
cd software

# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
echo export PATH="$HOME/miniconda/bin:\$PATH" >> ~/.profile
. ~/.profile

# install snakemake
conda install --yes pandas
conda install --yes -c bioconda -c conda-forge snakemake

# Install parallel
wget http://ftp.gnu.org/gnu/parallel/parallel-20180722.tar.bz2
tar xvjf parallel-20180722.tar.bz2

# Bin
mkdir -p ~/bin
cd ~/bin
ln -s ../software/parallel-20180722/src/parallel
echo export PATH="$HOME/bin:\$PATH" >> ~/.profile
. ~/.profile

echo COMPLETE
