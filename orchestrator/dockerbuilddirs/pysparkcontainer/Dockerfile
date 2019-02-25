FROM pythoncontainer

COPY install.sh /usr/bin/install.sh

ENV JAVA_HOME=/opt/jdk    \
    SPARK_HOME=/opt/spark \
    LD_LIBRARY_PATH=/opt/hadoop/lib/native:$LD_LIBRARY_PATH \
    PATH=/opt/jdk/bin:/opt/spark/bin:/opt/hadoop/bin:$PATH

RUN chmod +x /usr/bin/install.sh && bash install.sh
