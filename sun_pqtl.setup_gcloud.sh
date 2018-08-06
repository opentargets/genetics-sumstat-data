#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p software
cd software

# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
echo export PATH="$HOME/miniconda/bin:\$PATH" >> ~/.profile
sleep 1
. ~/.profile

# install snakemake
# conda install --yes pandas
conda install --yes -c bioconda -c conda-forge snakemake pypy3.5 pandas

# Pypy3
# wget https://bitbucket.org/pypy/pypy/downloads/pypy3-v6.0.0-linux64.tar.bz2
# tar xvjf pypy3-v6.0.0-linux64.tar.bz2

# Bin
# mkdir -p ~/bin
# cd ~/bin
# ln -s ../software/pypy3-v6.0.0-linux64/bin/pypy3
# echo export PATH="$HOME/bin:\$PATH" >> ~/.profile
# sleep 1
# . ~/.profile

echo COMPLETE
