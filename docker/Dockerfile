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
#   Run the docker command below
#      docker build --build-arg MVN_ARG="-Pmirror-cn" -f docker/Dockerfile -t apache/kyuubi:tagname .
#   Options:
#     -f this docker file
#     -t the target repo and tag name
#     more options can be found with -h

FROM maven:3.6-jdk-8 as builder

ARG MVN_ARG

ADD . /workspace/kyuubi
WORKDIR /workspace/kyuubi

RUN ./build/dist ${MVN_ARG} && \
    mv /workspace/kyuubi/dist /opt/kyuubi && \
    rm -rf ~/.m2 && \
    rm -rf /workspace/kyuubi

FROM openjdk:8-jdk

ARG kyuubi_uid=10009

USER root

ENV KYUUBI_HOME /opt/kyuubi
ENV KYUUBI_LOG_DIR ${KYUUBI_HOME}/logs
ENV KYUUBI_PID_DIR ${KYUUBI_HOME}/pid
ENV KYUUBI_WORK_DIR_ROOT ${KYUUBI_HOME}/work

COPY --from=builder /opt/kyuubi ${KYUUBI_HOME}

RUN set -ex && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt install -y bash tini libc6 libpam-modules krb5-user libnss3 procps && \
    useradd -u ${kyuubi_uid} -g root kyuubi && \
    mkdir -p ${KYUUBI_HOME} ${KYUUBI_LOG_DIR} ${KYUUBI_PID_DIR} ${KYUUBI_WORK_DIR_ROOT} && \
    chmod ug+rw -R ${KYUUBI_HOME} && \
    chmod a+rwx -R ${KYUUBI_WORK_DIR_ROOT} && \
    rm -rf /var/cache/apt/*

WORKDIR ${KYUUBI_HOME}

CMD [ "./bin/kyuubi", "run" ]

USER ${kyuubi_uid}
