/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache

import java.util.Properties

import scala.util.Try

package object kyuubi {
  private object BuildInfo {
    private val buildFile = "kyuubi-version-info.properties"
    private val buildFileStream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)

    if (buildFileStream == null) {
      throw new KyuubiException(s"Can not load the core build file: $buildFile, if you meet " +
        s"this exception when running unit tests " +
        s"please make sure you have run the `mvn package` or " +
        s"`mvn antrun:run@build-info -pl kyuubi-common` command.")
    }

    private val unknown = "<unknown>"
    private val props = new Properties()

    try {
      props.load(buildFileStream)
    } finally {
      Try(buildFileStream.close())
    }

    private def getProperty(key: String, defaultValue: String = unknown): String = {
      Option(props.getProperty(key, defaultValue)).filterNot(_.isEmpty).getOrElse(defaultValue)
    }

    val version: String = getProperty("kyuubi_version")
    val java_version: String = getProperty("kyuubi_java_version")
    val scala_version: String = getProperty("kyuubi_scala_version")
    val spark_version: String = getProperty("kyuubi_spark_version")
    val hive_version: String = getProperty("kyuubi_hive_version")
    val hadoop_version: String = getProperty("kyuubi_hadoop_version")
    val flink_version: String = getProperty("kyuubi_flink_version")
    val trino_version: String = getProperty("kyuubi_trino_version")
    val branch: String = getProperty("branch")
    val revision: String = getProperty("revision")
    val revisionTime: String = getProperty("revision_time")
    val user: String = getProperty("user")
    val repoUrl: String = getProperty("url")
    val buildDate: String = getProperty("date")
  }

  val KYUUBI_VERSION: String = BuildInfo.version
  val JAVA_COMPILE_VERSION: String = BuildInfo.java_version
  val SCALA_COMPILE_VERSION: String = BuildInfo.scala_version
  val SPARK_COMPILE_VERSION: String = BuildInfo.spark_version
  val HIVE_COMPILE_VERSION: String = BuildInfo.hive_version
  val HADOOP_COMPILE_VERSION: String = BuildInfo.hadoop_version
  val FLINK_COMPILE_VERSION: String = BuildInfo.flink_version
  val TRINO_COMPILE_VERSION: String = BuildInfo.trino_version
  val BRANCH: String = BuildInfo.branch
  val REVISION: String = BuildInfo.revision
  val REVISION_TIME: String = BuildInfo.revisionTime
  val BUILD_USER: String = BuildInfo.user
  val REPO_URL: String = BuildInfo.repoUrl
  val BUILD_DATE: String = BuildInfo.buildDate
}
