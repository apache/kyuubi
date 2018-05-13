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

package yaooqinn

import java.io.FileNotFoundException
import java.util.Properties

package object kyuubi {

  private object BuildInfo extends Logging {

    val (kyuubi_version, spark_version, branch, revision, user, repo_url, build_date) = {
      val buildFile = "kyuubi-version-info.properties"
      Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)) match {
        case Some(res) =>
          try {
            val unknown = "<unknown>"
            val props = new Properties()
            props.load(res)
            (
              props.getProperty("kyuubi_version", unknown),
              props.getProperty("spark_version", unknown),
              props.getProperty("branch", unknown),
              props.getProperty("revision", unknown),
              props.getProperty("user", unknown),
              props.getProperty("url", unknown),
              props.getProperty("date", unknown)
            )
          } catch {
            case e: Exception => throw e
          } finally {
            try {
              res.close()
            } catch {
              case e: Exception => throw e
            }
          }

        case _ =>
          val msg = s"Could not find $buildFile"
          error(msg)
          throw new FileNotFoundException(msg)
      }
    }
  }

  val KYUUBI_VERSION = BuildInfo.kyuubi_version
  val SPARK_COMPILE_VERSION = BuildInfo.spark_version
  val BRANCH = BuildInfo.branch
  val REVISION = BuildInfo.revision
  val BUILD_USER = BuildInfo.user
  val REPO_URL = BuildInfo.repo_url
  val BUILD_DATE = BuildInfo.build_date
}
