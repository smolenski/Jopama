FROM openjdk:8
MAINTAINER Maciej Smolenski <jezdnia@gmail.com>
RUN mkdir /opt/ZooKeeper
ADD zookeeper-3.4.9.tar.gz /opt/ZooKeeper/ 
ADD runZK.sh /opt/ZooKeeper/
ENTRYPOINT [ "/opt/ZooKeeper/runZK.sh", "/opt/ZooKeeper/zookeeper-3.4.9", "/var/jopamaTest/logs", "/var/jopamaTest/storage" ]
