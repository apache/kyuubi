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
    private val unknown = "<unknown>"
    private val props = new Properties()

    try {
      props.load(buildFileStream)
    } finally {
      Try(buildFileStream.close())
    }

    val version: String = props.getProperty("kyuubi_version", unknown)
    val java_version: String = props.getProperty("kyuubi_java_version", unknown)
    val scala_version: String = props.getProperty("kyuubi_scala_version", unknown)
    val spark_version: String = props.getProperty("kyuubi_spark_version", unknown)
    val hive_version: String = props.getProperty("kyuubi_hive_version", unknown)
    val hadoop_version: String = props.getProperty("kyuubi_hadoop_version", unknown)
    val branch: String = props.getProperty("branch", unknown)
    val revision: String = props.getProperty("revision", unknown)
    val user: String = props.getProperty("user", unknown)
    val repoUrl: String = props.getProperty("url", unknown)
    val buildDate: String = props.getProperty("date", unknown)
  }

  val KYUUBI_VERSION: String = BuildInfo.version
  val JAVA_COMPILE_VERSION: String = BuildInfo.java_version
  val SCALA_COMPILE_VERSION: String = BuildInfo.scala_version
  val SPARK_COMPILE_VERSION: String = BuildInfo.spark_version
  val HIVE_COMPILE_VERSION: String = BuildInfo.hive_version
  val HADOOP_COMPILE_VERSION: String = BuildInfo.hadoop_version
  val BRANCH: String = BuildInfo.branch
  val REVISION: String = BuildInfo.revision
  val BUILD_USER: String = BuildInfo.user
  val REPO_URL: String = BuildInfo.repoUrl
  val BUILD_DATE: String = BuildInfo.buildDate
}
