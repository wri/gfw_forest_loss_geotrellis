#!/bin/bash

# This is the bootstrap script used to install GDAL on individual EMR nodes before
# they run.

set -ex

# The default GDAL version would be 3.8.3
GDAL_VERSION=$1
GDAL_VERSION=${GDAL_VERSION:="3.8.3"}

# Install GDAL using Miniconda3 (which includes Python 3.12 by default)
# Avoid using default anaconda repo.

wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
sudo sh Miniconda3-latest-Linux-x86_64.sh -b -p /usr/local/miniconda

source ~/.bashrc
export PATH=/usr/local/miniconda/bin:$PATH

sudo pip3 install tqdm six && \
sudo /usr/local/miniconda/bin/conda install -c conda-forge  --override-channels hdf5 libnetcdf gdal=${GDAL_VERSION} -y

echo "export PATH=/usr/local/miniconda/bin:$PATH" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=/usr/local/miniconda/lib/:/usr/local/lib:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib" >> ~/.bashrc
