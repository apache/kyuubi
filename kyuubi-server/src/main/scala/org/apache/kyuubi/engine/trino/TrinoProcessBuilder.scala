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

package org.apache.kyuubi.engine.trino

import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_CONNECTION_URL
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_MAIN_RESOURCE
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.trino.TrinoProcessBuilder._
import org.apache.kyuubi.operation.log.OperationLog

class TrinoProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None) extends ProcBuilder with Logging {

  private[trino] lazy val trinoConf: Map[String, String] = {
    assert(
      conf.get(ENGINE_TRINO_CONNECTION_URL).isDefined,
      throw KyuubiSQLException(
        s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}"))
    assert(
      conf.get(ENGINE_TRINO_CONNECTION_CATALOG).isDefined,
      throw KyuubiSQLException(
        s"Trino default catalog can not be null!" +
          s" Please set ${ENGINE_TRINO_CONNECTION_CATALOG.key}"))

    conf.getAll.filter { case (k, v) =>
      !k.startsWith("spark.") && !k.startsWith("hadoop.")
    } + (USER -> proxyUser)
  }

  override protected val executable: String = {
    val trinoHomeOpt = env.get("TRINO_ENGINE_HOME").orElse {
      val cwd = Utils.getCodeSourceLocation(getClass)
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("externals")
          .resolve(module)
          .toFile)
        .map(_.getAbsolutePath)
    }

    trinoHomeOpt.map { dir =>
      Paths.get(dir, "bin", TRINO_ENGINE_BINARY_FILE).toAbsolutePath.toFile.getCanonicalPath
    }.getOrElse {
      throw KyuubiSQLException("TRINO_ENGINE_HOME is not set! " +
        "For more detail information on installing and configuring Trino, please visit " +
        "https://kyuubi.apache.org/docs/latest/deployment/settings.html#environments")
    }
  }

  override protected def mainResource: Option[String] = {
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    // 1. get the main resource jar for user specified config first
    conf.get(ENGINE_TRINO_MAIN_RESOURCE).filter { userSpecified =>
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
        .map { Paths.get(_, "externals", "engines", "trino", "jars", jarName) }
        .filter(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      Option(Paths.get("externals", module, "target", jarName))
        .filter(Files.exists(_)).orElse {
          Some(Paths.get("..", "externals", module, "target", jarName))
        }.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  override protected def module: String = "kyuubi-trino-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.trino.TrinoSqlEngine"

  override protected def childProcEnv: Map[String, String] = conf.getEnvs +
    ("TRINO_ENGINE_JAR" -> mainResource.get) +
    ("TRINO_ENGINE_DYNAMIC_ARGS" ->
      trinoConf.map { case (k, v) => s"-D$k=$v" }.mkString(" "))

  override protected def commands: Array[String] = Array(executable)
}

object TrinoProcessBuilder {
  final private val USER = "kyuubi.trino.user"

  final private val TRINO_ENGINE_BINARY_FILE = "trino-engine.sh"
}
