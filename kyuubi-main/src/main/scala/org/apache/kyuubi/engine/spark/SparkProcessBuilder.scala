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
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.ProcessBuilderLike

class SparkProcessBuilder(
    conf: Map[String, String],
    override val proxyUser: Option[String],
    override val mainResource: Option[String])
  extends ProcessBuilderLike {

  import SparkProcessBuilder._

  override protected val executable: String = {
    var sparkHome = conf.getOrElse("SPARK_HOME", System.getenv("SPARK_HOME"))

    if (sparkHome == null) {
      sparkHome = "./externals/kyuubi-download/target/spark-3.0.0-bin-hadoop2.7"
    }

    val exec = Seq(sparkHome, "bin", "spark-submit").mkString(File.separator)
    require(new File(exec).exists(), "Please specific SPARK_HOME environment variable to a" +
      " valid spark release package")
    exec
  }

  override val mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

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
object SparkProcessBuilder extends Logging {

  private final val CONF = "--conf"
  private final val CLASS = "--class"
  private final val PROXY_USER = "--proxy-user"

  def main(args: Array[String]): Unit = {
    val conf = Map("spark.abc" -> "1", "spark.xyz" -> "2")
    val sparkProcessBuilder = new SparkProcessBuilder(
      conf,
      Some("kent"),
      Some("externals/kyuubi-spark-sql-engine/target/kyuubi-spark-sql-engine-1.0.0-SNAPSHOT.jar"))
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
