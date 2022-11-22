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

import os

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters
from py4j.java_gateway import java_import, JavaGateway, GatewayParameters
from pyspark.context import SparkContext
from pyspark.serializers import read_int, UTF8Deserializer
from pyspark.sql import SparkSession


def connect_to_exist_gateway() -> "JavaGateway":
    conn_info_file = os.environ.get("PYTHON_GATEWAY_CONNECTION_INFO")
    if conn_info_file is None:
        raise SystemExit("the python gateway connection information file not found!")
    with open(conn_info_file, "rb") as info:
        gateway_port = read_int(info)
        gateway_secret = UTF8Deserializer().loads(info)
    if os.environ.get("PYSPARK_PIN_THREAD", "true").lower() == "true":
        gateway = ClientServer(
            java_parameters=JavaParameters(
                port=gateway_port, auth_token=gateway_secret, auto_convert=True
            ),
            python_parameters=PythonParameters(port=0, eager_load=False),
        )
    else:
        gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                port=gateway_port, auth_token=gateway_secret, auto_convert=True
            )
        )
    # gateway.proc = proc

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.resource.*")
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway


def _get_exist_spark_context(self, jconf):
    """
    Initialize SparkContext in function to allow subclass specific initialization
    """
    return self._jvm.JavaSparkContext(
        self._jvm.org.apache.spark.SparkContext.getOrCreate(jconf)
    )


def get_spark_session() -> "SparkSession":
    SparkContext._initialize_context = _get_exist_spark_context
    gateway = connect_to_exist_gateway()
    SparkContext._ensure_initialized(gateway=gateway)
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    return spark
