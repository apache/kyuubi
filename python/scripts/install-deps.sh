#!/bin/bash -eux

source /etc/lsb-release

sudo apt-get -q update
sudo apt-get -q install -y g++ libsasl2-dev libkrb5-dev

pip install --upgrade pip
pip install -r dev_requirements.txt
pip install -e .
