#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script is inspired by Apache Spark

set -x

echo "=================================="
echo "Free up disk space on CI system"
echo "=================================="

echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100
df -h

echo "Removing large packages"
sudo rm -rf /usr/share/dotnet/ \
  /usr/share/php/ \
  /usr/local/graalvm/ \
  /usr/local/.ghcup/ \
  /usr/local/share/powershell \
  /usr/local/share/chromium \
  /usr/local/lib/android \
  /usr/local/lib/node_modules \
  /opt/az \
  /opt/hostedtoolcache/CodeQL \
  /opt/hostedtoolcache/go \
  /opt/hostedtoolcache/node

df -h

sudo apt-get remove --purge -y \
  '^aspnet.*' \
  '^dotnet-.*' \
  '^llvm.*' \
  'php.*' \
  '^temurin-\d{n,}.*' \
  snapd google-chrome-stable microsoft-edge-stable firefox \
  azure-cli google-cloud-sdk mono-devel msbuild powershell libgl1-mesa-dri

df -h

sudo apt-get autoremove --purge -y \
  && sudo apt-get clean \
  && sudo apt-get autoclean

df -h
