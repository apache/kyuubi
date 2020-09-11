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

package org.apache.kyuubi.engine.spark

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.EngineConf.ENGINE_SPARK_MAIN_RESOURCE
import org.apache.kyuubi.engine.ProcessBuilderLike

class SparkProcessBuilder(
    override val proxyUser: Option[String],
    conf: Map[String, String],
    override val env: Map[String, String] = sys.env)
  extends ProcessBuilderLike {

  import SparkProcessBuilder._

  override protected val executable: String = {
    val path = env.get("SPARK_HOME").map { sparkHome =>
      Paths.get(sparkHome, "bin", "spark-submit").toAbsolutePath
    } getOrElse {
      Paths.get(
        "..",
        "externals",
        "kyuubi-download",
        "target",
        s"spark-$SPARK_COMPILE_VERSION-bin-hadoop2.7",
        "bin", "spark-submit").toAbsolutePath
    }
    path.toString
  }

  override val mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  override val mainResource: Option[String] = {
    // 1. get the main resource jar for user specified config first
    conf.get(ENGINE_SPARK_MAIN_RESOURCE.key).filter { userSpecified =>
      Files.exists(Paths.get(userSpecified))
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KyuubiConf.KYUUBI_HOME)
        .map {
          Paths.get(
            _,
            "externals",
            "engines",
            "spark",
            s"kyuubi-spark-sql-engine-$KYUUBI_VERSION.jar")
        }.filter(Files.exists(_)).map(_.toAbsolutePath.toString)
    }.orElse {
      // 3. get the main resource from dev environment
      Some(Paths.get(
        "..",
        "externals",
        "kyuubi-spark-sql-engine",
        "target",
        s"kyuubi-spark-sql-engine-$KYUUBI_VERSION.jar").toAbsolutePath.toString)
    }
  }

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass
    conf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"$k=$v"
    }

    proxyUser.foreach { u =>
      buffer += PROXY_USER
      buffer += u
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

  override def toString: String = commands.mkString(" ")
}


/**
 * May need download spark release packages first.
 *
 * (build/)mvn clean package -pl :kyuubi-download -DskipTests
 */
object SparkProcessBuilder {

  private final val CONF = "--conf"
  private final val CLASS = "--class"
  private final val PROXY_USER = "--proxy-user"

  def main(args: Array[String]): Unit = {
    val conf = Map("spark.abc" -> "1", "spark.xyz" -> "2")
    val sparkProcessBuilder = new SparkProcessBuilder(Some("kent"), conf)
    print(sparkProcessBuilder.toString)
    val file = new File(s"${UUID.randomUUID()}abc.log")
    file.createNewFile()
    file.deleteOnExit()
    sparkProcessBuilder.processBuilder.redirectOutput(file)
    sparkProcessBuilder.processBuilder.redirectError(file)
    val start = sparkProcessBuilder.start

    start.waitFor()

  }
}
