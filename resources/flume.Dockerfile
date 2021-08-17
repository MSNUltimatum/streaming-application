FROM centos

MAINTAINER xyu <info@paris8.org>

USER root

# Setup OpenJDK
RUN yum -y update
RUN yum -y remove java
RUN yum install -y wget
RUN yum install -y \
       java-1.8.0-openjdk \
       java-1.8.0-openjdk-devel


# Setup Flume -------------------------------------------------------------------------------
ENV FLUME_HOME=/root/flume
ENV PATH=$PATH:$FLUME_HOME/bin:.

RUN mkdir "requests"

RUN wget http://apache.mirrors.ovh.net/ftp.apache.org/dist/flume/1.8.0/apache-flume-1.8.0-bin.tar.gz && \
	tar -xzvf apache-flume-1.8.0-bin.tar.gz -C /root/ && \
	mv /root/apache-flume-1.8.0-bin $FLUME_HOME && \
	rm -rf apache-flume-1.8.0-bin.tar.gz

EXPOSE 40000

COPY /resources/flume.conf $FLUME_HOME/conf/
COPY ../flume-filter/target/flume-filter-1.0-SNAPSHOT.jar $FLUME_HOME/lib

ADD /resources/start-flume.sh $FLUME_HOME/bin/start-flume

ENTRYPOINT [ "start-flume" ]