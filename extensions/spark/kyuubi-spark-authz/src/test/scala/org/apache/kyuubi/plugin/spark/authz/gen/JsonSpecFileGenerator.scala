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

package org.apache.kyuubi.plugin.spark.authz.gen

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.kyuubi.plugin.spark.authz.serde.{mapper, CommandSpec}

/**
 * Generates the default command specs to src/main/resources dir.
 *
 * Usage:
 * mvn scala:run -DmainClass=this class -pl :kyuubi-spark-authz_2.12
 */
object JsonSpecFileGenerator {

  def main(args: Array[String]): Unit = {
    write(DatabaseCommands.data, "database")
    write(TableCommands.data ++ IcebergCommands.data, "table")
    write(FunctionCommands.data, "function")
  }

  def write[T <: CommandSpec](data: Array[T], filename: String): Unit = {
    val pluginHome = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      .split("target").head
    val writer = {
      val p =
        Paths.get(pluginHome, "src", "main", "resources", s"${filename}_command_spec.json")
      Files.newBufferedWriter(p, StandardCharsets.UTF_8)
    }
    mapper.writerWithDefaultPrettyPrinter().writeValue(writer, data.sortBy(_.classname))
    writer.close()
  }
}
