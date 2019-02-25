FROM ubuntu:16.04

COPY Anaconda.sh /opt/Anaconda.sh

COPY install.sh /usr/bin/install.sh

ENV PATH=/opt/anaconda/bin:/opt/gcsdk/bin:$PATH \
    CLOUDSDK_PYTHON=python2.7 \
    LANGUAGE=en_US.UTF-8      \
    LANG=en_US.UTF-8          \
    LC_ALL=C.UTF-8            \
    TERM=linux

RUN chmod +x /usr/bin/install.sh && bash install.sh
