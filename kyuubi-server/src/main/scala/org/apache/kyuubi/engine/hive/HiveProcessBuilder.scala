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

package org.apache.kyuubi.engine.hive

import java.net.URI
import java.nio.file.{Files, Paths}

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, SCALA_COMPILE_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_HIVE_MAIN_RESOURCE
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.hive.HiveProcessBuilder.HIVE_ENGINE_BINARY_FILE
import org.apache.kyuubi.operation.log.OperationLog

class HiveProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  override protected def executable: String = {
    val hiveHomeOpt = env.get("HIVE_ENGINE_HOME").orElse {
      val cwd = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("externals")
          .resolve(module)
          .toFile)
        .map(_.getAbsolutePath)
    }

    hiveHomeOpt.map { dir =>
      Paths.get(dir, "bin", HIVE_ENGINE_BINARY_FILE).toAbsolutePath.toFile.getCanonicalPath
    }.getOrElse {
      throw KyuubiSQLException("HIVE_ENGINE_HOME is not set! " +
        "For more detail information on installing and configuring Hive, please visit " +
        "https://kyuubi.apache.org/docs/stable/deployment/settings.html#environments")
    }
  }

  override protected def mainResource: Option[String] = {
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    // 1. get the main resource jar for user specified config first
    conf.get(ENGINE_HIVE_MAIN_RESOURCE).filter { userSpecified =>
      // skip check exist if not local file.
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KyuubiConf.KYUUBI_HOME)
        .map { Paths.get(_, "externals", "engines", "hive", "jars", jarName) }
        .filter(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      Option(Paths.get("externals", module, "target", jarName))
        .filter(Files.exists(_)).orElse {
          Some(Paths.get("..", "externals", module, "target", jarName))
        }.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  override protected def childProcEnv: Map[String, String] = conf.getEnvs +
    ("HIVE_ENGINE_JAR" -> mainResource.get) +
    ("HIVE_ENGINE_DYNAMIC_ARGS" ->
      conf.getAll.map { case (k, v) => s"-D$k=$v" }.mkString(" "))

  override protected def module: String = "kyuubi-hive-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.hive.HiveSQLEngine"

  override protected def commands: Array[String] = Array(executable)

}

object HiveProcessBuilder {

  val HIVE_ENGINE_BINARY_FILE = "hive-sql-engine.sh"

}
