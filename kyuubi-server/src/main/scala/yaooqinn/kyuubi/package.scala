/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn

import java.util.Properties

import scala.util.Try

import yaooqinn.kyuubi.service.ServiceException

package object kyuubi {
  private object BuildInfo {
    private val buildFile = "kyuubi-version-info.properties"
    private val buildFileStream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)
    private val unknown = "<unknown>"
    private val props = new Properties()

    try {
      props.load(buildFileStream)
    } catch {
      case e: Exception => throw new ServiceException(e)
    } finally {
      Try(buildFileStream.close())
    }

    val version: String = props.getProperty("kyuubi_version", unknown)
    val sparkVersion: String = props.getProperty("spark_version", unknown)
    val branch: String = props.getProperty("branch", unknown)
    val jar: String = props.getProperty("jar", unknown)
    val revision: String = props.getProperty("revision", unknown)
    val user: String = props.getProperty("user", unknown)
    val repoUrl: String = props.getProperty("url", unknown)
    val buildDate: String = props.getProperty("date", unknown)
  }

  val KYUUBI_VERSION: String = BuildInfo.version
  val SPARK_COMPILE_VERSION: String = BuildInfo.sparkVersion
  val BRANCH: String = BuildInfo.branch
  val KYUUBI_JAR_NAME: String = BuildInfo.jar
  val REVISION: String = BuildInfo.revision
  val BUILD_USER: String = BuildInfo.user
  val REPO_URL: String = BuildInfo.repoUrl
  val BUILD_DATE: String = BuildInfo.buildDate
}
