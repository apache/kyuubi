# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG KYUUBI_VERSION

FROM nekyuubi/kyuubi-playground-spark:${KYUUBI_VERSION}

ARG AWS_JAVA_SDK_VERSION
ARG KYUUBI_VERSION
ARG KYUUBI_HADOOP_VERSION

ARG APACHE_MIRROR
ARG MAVEN_MIRROR

ENV KYUUBI_HOME=/opt/kyuubi
ENV KYUUBI_CONF_DIR=/etc/kyuubi/conf

RUN set -x && \
    wget -q ${APACHE_MIRROR}/kyuubi/kyuubi-${KYUUBI_VERSION}/apache-kyuubi-${KYUUBI_VERSION}-bin.tgz && \
    tar -xzf apache-kyuubi-${KYUUBI_VERSION}-bin.tgz -C /opt && \
    ln -s /opt/apache-kyuubi-${KYUUBI_VERSION}-bin ${KYUUBI_HOME} && \
    rm apache-kyuubi-${KYUUBI_VERSION}-bin.tgz && \
    HADOOP_AWS_JAR_NAME=hadoop-aws && \
    wget -q ${MAVEN_MIRROR}/org/apache/hadoop/${HADOOP_AWS_JAR_NAME}/${KYUUBI_HADOOP_VERSION}/${HADOOP_AWS_JAR_NAME}-${KYUUBI_HADOOP_VERSION}.jar -P ${KYUUBI_HOME}/jars && \
    AWS_JAVA_SDK_BUNDLE_JAR_NAME=aws-java-sdk-bundle && \
    wget -q ${MAVEN_MIRROR}/com/amazonaws/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}/${AWS_JAVA_SDK_VERSION}/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}-${AWS_JAVA_SDK_VERSION}.jar -P ${KYUUBI_HOME}/jars && \
    useradd anonymous

ENTRYPOINT ["/opt/kyuubi/bin/kyuubi", "run"]
