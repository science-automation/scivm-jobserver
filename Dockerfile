FROM ubuntu

RUN echo "deb ftp://mirror.hetzner.de/ubuntu/packages precise main restricted universe multiverse" > /etc/apt/sources.list;\
  apt-get update

# locale
RUN locale-gen en_US en_US.UTF-8

# Utilities
RUN apt-get install -y python-dev python-setuptools vim-tiny less curl git unzip subversion python-software-properties make gcc redis-server

# Supervisord
RUN apt-get install -y supervisor && mkdir -p /var/log/supervisor

# SSHD
RUN apt-get install -y openssh-server && mkdir /var/run/sshd

# job management
RUN mkdir -p /opt/apps/scivm/jobserver
ADD jobserver /opt/apps/scivm/jobserver

# supervisor
ADD supervisord.conf /etc/supervisor/supervisord.conf

# pip
RUN wget https://raw.github.com/pypa/pip/master/contrib/get-pip.py
RUN (python get-pip.py; rm -f /get-pip.py)
RUN pip install setuptools --no-use-wheel --upgrade
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# call a run script that will pull the latest worker code 
# and then start supervisor
RUN (mkdir -p /opt/apps/scivm/.docker)
ADD run.sh /opt/apps/scivm/.docker/run.sh
RUN (chmod 755 /opt/apps/scivm/.docker/run.sh)
workdir /opt/apps/scivm
entrypoint ["/opt/apps/scivm/.docker/run.sh"]

EXPOSE 22 8080
