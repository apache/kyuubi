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

# Usage:
#   Grafana is introduced to visualize and monitor metrics from Prometheus.
#   Here we provide a solution for users to easily build a grafana image.
#   Data sources and dashboards both are defined and managed via yml files.
#   Below is the script to build a grafana image with grafana dashboards(json format files) that are predefined
#   under the grafana folder.
#   1. docker build --build-arg PROMETHEUS_URL_ARG="" -t grafana:kyuubi -f Dockerfile .
#   Options:
#     --build-arg the url to access promethues datasource
#     -t the target repo and tag name
#     -f this docker file
#     more options can be found with -h
#   Once the grafana starts up, it will load all datasource and dashboars according to config files(dashboard.yml and datasource.yml)
FROM grafana/grafana:9.5.7
ARG PROMETHEUS_URL_ARG
ENV PROMETHEUS_URL=$PROMETHEUS_URL_ARG
COPY ./dashboard.yml /etc/grafana/provisioning/dashboards/
COPY ./datasource.yml  /etc/grafana/provisioning/datasources/
COPY ./*.json /var/lib/grafana/dashboards/