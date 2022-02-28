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

import java.nio.file.Paths

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder._
import org.apache.kyuubi.operation.log.OperationLog

class HiveProcBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    override val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder {

  override protected val executable: String = {
    Paths.get(
      resolveSparkHome(env),
      "bin",
      "spark-class").toFile.getAbsolutePath
  }
  override protected def mainResource: Option[String] = None

  override protected def module: String = "kyuubi-hive-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.hive.HiveSQLEngine"

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    conf.getAll.foreach { case (k, v) =>
      val newKey =
        if (k.startsWith("spark.")) {
          k
        } else if (k.startsWith("hadoop.")) {
          "spark.hadoop." + k
        } else {
          "spark." + k
        }
      buffer += CONF
      buffer += s"$newKey=$v"
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray

  }

}
