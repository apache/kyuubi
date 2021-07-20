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
#   docker build -f tools/kyuubi-shuffle-service/kubernetes/docker/Dockerfile -t yaooqinn/kyuubi-shuffle-service:tagname .
#   docker push yaooqinn/kyuubi-shuffle-service:tagname

# TODO: REPALCE it with offical spark image iff Apache Spark community deploy
# Or make one after kyuubi to be setup under ASF

ARG SPARK_IMAGE=yaooqinn/spark:3.1.2
FROM ${SPARK_IMAGE}

CMD [ "../bin/spark-class", "org.apache.spark.deploy.ExternalShuffleService" ]
