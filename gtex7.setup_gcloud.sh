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
conda install --yes -c bioconda -c conda-forge snakemake pandas

echo COMPLETE
