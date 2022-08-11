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

package org.apache.kyuubi.engine.jdbc

import java.io.File
import java.nio.file.Paths
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.Utils.REDACTION_REPLACEMENT_TEXT
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_PASSWORD, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_EXTRA_CLASSPATH, ENGINE_JDBC_JAVA_OPTIONS, ENGINE_JDBC_MEMORY}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog

class JdbcProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  /**
   * The short name of the engine process builder, we use this for form the engine jar paths now
   * see `mainResource`
   */
  override def shortName: String = "jdbc"

  override protected def module: String = "kyuubi-jdbc-engine"

  /**
   * The class containing the main method
   */
  override protected def mainClass: String = "org.apache.kyuubi.engine.jdbc.JdbcSQLEngine"

  override protected val commands: Array[String] = {
    require(
      conf.get(ENGINE_JDBC_CONNECTION_URL).nonEmpty,
      s"Jdbc server url can not be null! Please set ${ENGINE_JDBC_CONNECTION_URL.key}")
    val buffer = new ArrayBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_JDBC_MEMORY)
    buffer += s"-Xmx$memory"

    val javaOptions = conf.get(ENGINE_JDBC_JAVA_OPTIONS)
    javaOptions.foreach(buffer += _)

    buffer += "-cp"
    val classpathEntries = new util.LinkedHashSet[String]
    mainResource.foreach(classpathEntries.add)
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val jdbcDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$jdbcDeps${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_JDBC_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    buffer += classpathEntries.asScala.mkString(File.pathSeparator)
    buffer += mainClass

    buffer += "--conf"
    buffer += s"$KYUUBI_SESSION_USER_KEY=$proxyUser"

    for ((k, v) <- conf.getAll) {
      buffer += "--conf"
      buffer += s"$k=$v"
    }
    buffer.toArray
  }

  override def toString: String = {
    if (commands == null) {
      super.toString()
    } else {
      Utils.redactCommandLineArgs(conf, commands).map {
        case arg if arg.contains(ENGINE_JDBC_CONNECTION_PASSWORD.key) =>
          s"${ENGINE_JDBC_CONNECTION_PASSWORD.key}=$REDACTION_REPLACEMENT_TEXT"
        case arg => arg
      }.mkString("\n")
    }
  }
}
