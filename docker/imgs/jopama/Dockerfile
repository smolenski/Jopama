FROM java
MAINTAINER Maciej Smolenski <jezdnia@gmail.com>
RUN mkdir /opt/Jopama
ADD Jopama.tar.gz /opt/Jopama/ 
ADD helpers.sh /opt/Jopama/
ADD runJopama.sh /opt/Jopama/
ENTRYPOINT [ "/opt/Jopama/runJopama.sh", "/opt/Jopama", "/var/jopamaTest/logs" ]
