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

package org.apache.kyuubi.engine.flink

import java.io.{File, FilenameFilter}
import java.lang.ProcessBuilder.Redirect
import java.nio.file.Paths
import java.util.LinkedHashSet

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.operation.log.OperationLog

/**
 * A builder to build flink sql engine progress.
 */
class FlinkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override protected def childProcEnv: Map[String, String] = conf.getEnvs +
    ("FLINK_HOME" -> FLINK_HOME) +
    ("FLINK_CONF_DIR" -> s"$FLINK_HOME/conf")

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable

    // TODO: How shall we deal with proxyUser,
    // user.name
    // kyuubi.session.user
    // or just leave it, because we can handle it at operation layer
    buffer += s"-D$KYUUBI_SESSION_USER_KEY=$proxyUser"

    // TODO: add Kyuubi.engineEnv.FLINK_ENGINE_MEMORY or kyuubi.engine.flink.memory to configure
    // -Xmx5g
    // java options
    for ((k, v) <- conf.getAll) {
      buffer += s"-D$k=$v"
    }

    buffer += "-cp"
    val classpathEntries = new LinkedHashSet[String]
    // flink engine runtime jar
    mainResource.foreach(classpathEntries.add)
    // flink sql client jar
    val flinkSqlClientPath = Paths.get(FLINK_HOME)
      .resolve("opt")
      .toFile
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.startsWith("flink-sql-client")
        }
      }).head.getAbsolutePath
    classpathEntries.add(flinkSqlClientPath)

    // jars from flink lib
    classpathEntries.add(s"$FLINK_HOME${File.separator}lib${File.separator}*")

    // classpath contains flink configurations, default to flink.home/conf
    classpathEntries.add(s"$FLINK_HOME")
    classpathEntries.add(env.getOrElse("FLINK_CONF_DIR", s"$FLINK_HOME${File.separator}conf"))
    // classpath contains hadoop configurations
    env.get("HADOOP_CONF_DIR").foreach(classpathEntries.add)
    env.get("YARN_CONF_DIR").foreach(classpathEntries.add)
    env.get("HBASE_CONF_DIR").foreach(classpathEntries.add)

    val hadoopCp = env.get("HADOOP_CLASSPATH")
    hadoopCp.foreach(path => classpathEntries.add(s"$path${File.separator}*"))
    if (hadoopCp.isEmpty) {
      mainResource.foreach { path =>
        val devHadoopJars = Paths.get(path).getParent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$devHadoopJars${File.separator}*")
      }

    }

    buffer += classpathEntries.asScala.mkString(File.pathSeparator)
    buffer += mainClass
    buffer.toArray
  }
  override def killApplication(clue: Either[String, String]): String = clue match {
    case Left(_) => ""
    case Right(line) => killApplicationByLog(line)
  }

  def killApplicationByLog(line: String = lastRowsOfLog.toArray.mkString("\n")): String = {
    "Job ID: .*".r.findFirstIn(line) match {
      case Some(jobIdLine) =>
        val jobId = jobIdLine.split("Job ID: ")(1).trim
        env.get("FLINK_HOME") match {
          case Some(flinkHome) =>
            val pb = new ProcessBuilder("/bin/sh", s"$flinkHome/bin/flink", "stop", jobId)
            pb.environment()
              .putAll(childProcEnv.asJava)
            pb.redirectError(Redirect.appendTo(engineLog))
            pb.redirectOutput(Redirect.appendTo(engineLog))
            val process = pb.start()
            process.waitFor() match {
              case id if id != 0 => s"Failed to kill Application $jobId, please kill it manually. "
              case _ => s"Killed Application $jobId successfully. "
            }
          case None =>
            s"FLINK_HOME is not set! Failed to kill Application $jobId, please kill it manually."
        }
      case None => ""
    }
  }

  @VisibleForTesting
  def FLINK_HOME: String = {
    // prepare FLINK_HOME
    val flinkHomeOpt = env.get("FLINK_HOME").orElse {
      val cwd = Utils.getCodeSourceLocation(getClass)
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("externals")
          .resolve("kyuubi-download")
          .resolve("target")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              dir.isDirectory && name.startsWith("flink-")
            }
          }))
        .flatMap(_.headOption)
        .map(_.getAbsolutePath)
    }

    flinkHomeOpt.map { dir =>
      dir
    } getOrElse {
      throw KyuubiSQLException("FLINK_HOME is not set! " +
        "For more detail information on installing and configuring Flink, please visit " +
        "https://kyuubi.apache.org/docs/latest/deployment/settings.html#environments")
    }
  }

  override protected def shortName: String = "flink"
}

object FlinkProcessBuilder {
  final val APP_KEY = "yarn.application.name"
  final val TAG_KEY = "yarn.tags"
}
