#!/bin/bash -eux

source /etc/lsb-release

sudo apt-get -q update
sudo apt-get -q install -y g++ libsasl2-dev libkrb5-dev

while ! nc -vz localhost 9083; do sleep 1; done
while ! nc -vz localhost 10000; do sleep 1; done

# sudo -Eu hive $(dirname $0)/make_test_tables.sh
