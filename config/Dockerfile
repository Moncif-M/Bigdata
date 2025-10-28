FROM ubuntu:bionic

MAINTAINER Yasser ALAMI <yassern1@gmail.com>

WORKDIR /root

# install openssh-server, openjdk vim python and wget
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server wget vim curl && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y \
        python3.8 python3.8-distutils python3.8-venv && \
    rm -rf /var/lib/apt/lists/*
    
#install scala & jupyter packages dependencies
RUN apt-get update && apt-get install -y wget gnupg && \
    wget https://downloads.lightbend.com/scala/2.13.14/scala-2.13.14.deb && \
    dpkg -i scala-2.13.14.deb || apt-get install -fy && \
    rm scala-2.13.14.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# Hadoop, Spark, Kafka, HBase installation
RUN set -eux; \
    wget -qO- https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz \
      | tar -xz -C /usr/local && mv /usr/local/hadoop-3.2.0 /usr/local/hadoop && \
    wget -qO- https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz \
      | tar -xz -C /usr/local && mv /usr/local/spark-3.2.4-bin-hadoop3.2 /usr/local/spark && \
    wget -qO- https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz \
      | tar -xz -C /usr/local && mv /usr/local/kafka_2.13-3.5.1 /usr/local/kafka && \
    wget -qO- https://archive.apache.org/dist/hbase/hbase-1.4.12/hbase-1.4.12-bin.tar.gz \
      | tar -xz -C /usr/local && mv /usr/local/hbase-1.4.12 /usr/local/hbase
 
# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
ENV HADOOP_HOME=/usr/local/hadoop 
ENV SPARK_HOME=/usr/local/spark
ENV KAFKA_HOME=/usr/local/kafka
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
ENV HBASE_HOME=/usr/local/hbase
ENV CLASSPATH=$CLASSPATH:/usr/local/hbase/lib/*
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/usr/local/kafka/bin:/usr/local/hbase/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/lib/jvm/java-8-openjdk-amd64/jre/bin

#ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH 

####new adds
ENV PYSPARK_PYTHON=/usr/bin/python3.8
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.8
# Add the alias to .bashrc
RUN echo "alias python='/usr/bin/python3.8'" >> ~/.bashrc


ENV CLASSPATH=/usr/local/kafka/

######



#source 
RUN . ~/.bashrc

# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs
	
COPY config/* /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    cp /tmp/workers $HADOOP_HOME/etc/hadoop/workers && \
    mv /tmp/workers $SPARK_HOME/conf/workers && \
    mv /tmp/start-kafka-zookeeper.sh ~/start-kafka-zookeeper.sh && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/start-spark.sh ~/start-spark.sh && \
    mv /tmp/run-wordcount.sh ~/run-wordcount.sh && \
    mv /tmp/start-jupyter.sh ~/start-jupyter.sh && \
    mv /tmp/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf && \
    mv /tmp/hbase-env.sh $HBASE_HOME/conf/hbase-env.sh && \
    mv /tmp/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x ~/start-kafka-zookeeper.sh && \
    chmod +x ~/start-jupyter.sh && \
    chmod +x ~/run-wordcount.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh 
#clear downloaded packages
#RUN pip cache purge && \
RUN apt-get clean && \
    apt-get autoremove --purge -y && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf ~/.cache/pip

# format namenode
RUN rm -rf /var/lib/apt/lists/*

RUN /usr/local/hadoop/bin/hdfs namenode -format

CMD [ "sh", "-c", "service ssh start; bash"]
