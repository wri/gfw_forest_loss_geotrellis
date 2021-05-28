FROM gcr.io/datamechanics/spark:platform-3.1.1-hadoop-3.2.0-java-11-scala-2.12-python-3.8-dm12

ARG VERSION

ENV SCALA_VERSION=2.12
ENV PYTHON_VERSION=3.8
ENV GDAL_VERSION=3.1.2

USER root

# Install GDAL using Conda
RUN wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh
RUN sh Miniconda-latest-Linux-x86_64.sh -b -p /usr/local/miniconda

ENV PATH=/usr/local/miniconda/bin:$PATH

RUN conda config --add channels conda-forge
RUN /usr/local/miniconda/bin/conda install python=${PYTHON_VERSION} -y
RUN pip install tqdm six
RUN /usr/local/miniconda/bin/conda install -c anaconda hdf5 -y
RUN /usr/local/miniconda/bin/conda install -c conda-forge libnetcdf gdal=${GDAL_VERSION} -y

ENV LD_LIBRARY_PATH=/usr/local/miniconda/lib/:/usr/local/lib:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native:/docker/usr/lib/hadoop/lib/native:/docker/usr/lib/hadoop-lzo/lib/native:/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib

# Copy Tree cover loss FAT JAR into docker containter
COPY target/scala-${SCALA_VERSION}/treecoverloss-assembly-${VERSION}.jar treecoverloss-assembly.jar
# Overwrite entrypoint
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod 755 /opt/entrypoint.sh

# Change back to inital user
USER 185