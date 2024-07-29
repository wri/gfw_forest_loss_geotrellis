# This Dockerfile is to create the image needed to run tests during
# .github/workflow/ciyaml. It includes openjdk-8, git, and gdal. For some reason,
# using Miniconda3 to load gdal broken the libraries needed for git, so we stick with
# old Miniconda here.
#
# Created via:
#  docker build -t test -f Dockerfile .
#  docker tag test:latest globalforestwatch/openjdk8-gdal3.8.3-slim:latest
#  docker login -u username
#  docker push globalforestwatch/openjdk8-gdal3.8.3-slim:latest

FROM openjdk:8

ENV SCALA_VERSION=2.12.12
ENV PYTHON_VERSION=3.8
ENV GDAL_VERSION=3.8.3

USER root

# Install GDAL using Conda
RUN wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
RUN bash Miniconda-latest-Linux-x86_64.sh -b -p /usr/local/miniconda

ENV PATH=/usr/local/miniconda/bin:$PATH

RUN conda config --add channels conda-forge && conda config --set ssl_verify false
RUN /usr/local/miniconda/bin/conda install python=${PYTHON_VERSION} -y
RUN pip install tqdm six
RUN /usr/local/miniconda/bin/conda install -c anaconda hdf5 -y
RUN /usr/local/miniconda/bin/conda install -c conda-forge libnetcdf gdal=${GDAL_VERSION} -y

ENV LD_LIBRARY_PATH=/usr/local/miniconda/lib/:/usr/local/lib:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib
ENV PROJ_LIB=/usr/local/miniconda/share/proj
