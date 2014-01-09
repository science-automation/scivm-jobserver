#!/bin/bash

# variables
APP_DIR=/opt/apps/scivm
DEBUG=${DEBUG:-True}
SUPERVISOR_CONF=/etc/supervisor/supervisord.conf
LOG_DIR=/var/log/scivm

# print the env
env

# check for worker manager ip address and port
SERVER_ADDRESS=${WM_ENV_SERVER_ADDRESS}
SERVER_PORT=${WM_ENV_SERVER_PORT}

# start supervisor
supervisord -c $SUPERVISOR_CONF -n
