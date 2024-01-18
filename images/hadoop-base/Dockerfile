FROM centos:7

RUN yum install wget net-tools telnet nc expect which -y \
    && yum install java-1.8.0-openjdk-devel.x86_64 -y

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk

RUN groupadd --system --gid=10000 hadoop && useradd --system --home-dir /home/hadoop --uid=10000 --gid=hadoop hadoop

RUN yum -y install sudo ; chmod 640 /etc/sudoers

RUN echo "hadoop ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

ENV HADOOP_HOME /opt/apache/hadoop

ENV HADOOP_COMMON_HOME=${HADOOP_HOME} \
    HADOOP_HDFS_HOME=${HADOOP_HOME} \
    HADOOP_MAPRED_HOME=${HADOOP_HOME} \
    HADOOP_YARN_HOME=${HADOOP_HOME} \
    HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    PATH=${PATH}:${HADOOP_HOME}/bin

RUN mkdir -p /opt/apache/ && \
    cd /opt/apache/ && \
    wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz --no-check-certificate \
    && tar -zxvf hadoop-3.3.6.tar.gz \
    && ln -s /opt/apache/hadoop-3.3.6 $HADOOP_HOME

RUN sudo mkdir -p /home/hadoop/ && sudo chown -R hadoop:hadoop /home/hadoop/

RUN chown -R hadoop:hadoop /opt/apache

WORKDIR $HADOOP_HOME