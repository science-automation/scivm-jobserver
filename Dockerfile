FROM ubuntu

RUN echo "deb ftp://mirror.hetzner.de/ubuntu/packages precise main restricted universe multiverse" > /etc/apt/sources.list;\
  apt-get update

# locale
RUN locale-gen en_US en_US.UTF-8

# Supervisord
RUN apt-get install -y supervisor && mkdir -p /var/log/supervisor

# SSHD
RUN apt-get install -y openssh-server && mkdir /var/run/sshd && echo 'root:root' |chpasswd

# Utilities
RUN apt-get install -y vim-tiny less ntp net-tools inetutils-ping curl git unzip subversion python-software-properties make gcc

# redis
RUN apt-get install -y redis-server

# job management

# supervisor
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["/usr/bin/supervisord"]

EXPOSE 22 8080
