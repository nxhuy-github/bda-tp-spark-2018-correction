FROM hseeberger/scala-sbt:8u181_2.12.7_1.2.4

# Install required packages
RUN apt update && apt -y install python-pip
RUN pip install pyspark==2.2.0.post0 pipenv pytest pytest-spark

# Get spark
RUN curl -fsL 'https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz' | tar xfz - -C /root/ && \
    echo >> /root/.bashrc && \
    echo "export SPARK_HOME=/root/spark-2.2.0-bin-hadoop2.7" >> /root/.bashrc
ENV SPARK_HOME=/root/spark-2.2.0-bin-hadoop2.7

# Download dependencies to avoid heavy downloads when running Scala builds
RUN mkdir /root/tmp-prj
RUN mkdir /root/tmp-prj/project
COPY build.sbt /root/tmp-prj
COPY assembly.sbt /root/tmp-prj/project
COPY build.properties /root/tmp-prj/project
RUN (cd /root/tmp-prj; sbt update)

