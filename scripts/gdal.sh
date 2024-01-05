#!/bin/bash

set -ex

# The default GDAL version would be 3.1.2
GDAL_VERSION=$1
GDAL_VERSION=${GDAL_VERSION:="3.1.2"}

# Install Conda
wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
sudo sh Miniconda-latest-Linux-x86_64.sh -b -p /usr/local/miniconda

source ~/.bashrc
export PATH=/usr/local/miniconda/bin:$PATH

# Install GDAL with a specific libcurl version (7.76.1)
conda config --add channels conda-forge
sudo pip3 install tqdm && \
sudo /usr/local/miniconda/bin/conda install python=3.6 -y && \
sudo /usr/local/miniconda/bin/conda install -c anaconda hdf5 -y && \
sudo /usr/local/miniconda/bin/conda install -c cctbx202105 libcurl=7.76.1 -y && \
sudo /usr/local/miniconda/bin/conda install -c conda-forge libnetcdf gdal=${GDAL_VERSION} -y

echo "export PATH=/usr/local/miniconda/bin:$PATH" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=/usr/local/miniconda/lib/:/usr/local/lib:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib" >> ~/.bashrc
