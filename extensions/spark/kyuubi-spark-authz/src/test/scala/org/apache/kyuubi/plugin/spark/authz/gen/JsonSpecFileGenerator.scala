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
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.commons.io.FileUtils
//scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.serde.{mapper, CommandSpec}

/**
 * Generates the default command specs to src/main/resources dir.
 *
 * To run the test suite:
 * build/mvn clean test -Pgen-policy -pl :kyuubi-spark-authz_2.12 -Dtest=none
 * -DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.gen.JsonSpecFileGenerator
 *
 * To regenerate the ranger policy file:
 * KYUUBI_UPDATE=1 build/mvn clean test -Pgen-policy -pl :kyuubi-spark-authz_2.12 -Dtest=none
 * -DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.gen.JsonSpecFileGenerator
 */

class JsonSpecFileGenerator extends AnyFunSuite {
  // scalastyle:on
  test("check spec json files") {
    writeCommandSpecJson("database", DatabaseCommands.data)
    writeCommandSpecJson("table", TableCommands.data ++ IcebergCommands.data)
    writeCommandSpecJson("function", FunctionCommands.data)
    writeCommandSpecJson("scan", Scans.data)
  }

  def writeCommandSpecJson[T <: CommandSpec](
      commandType: String,
      specArr: Array[T]): Unit = {
    val pluginHome = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
      .split("target").head
    val filename = s"${commandType}_command_spec.json"
    val filePath = Paths.get(pluginHome, "src", "main", "resources", filename)

    val generatedStr = mapper.writerWithDefaultPrettyPrinter()
      .writeValueAsString(specArr.sortBy(_.classname))

    if (sys.env.get("KYUUBI_UPDATE").contains("1")) {
      // scalastyle:off println
      println(s"writing ${specArr.length} specs to $filename")
      // scalastyle:on println
      Files.write(
        filePath,
        generatedStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      val existedFileContent =
        FileUtils.readFileToString(filePath.toFile, StandardCharsets.UTF_8)
      withClue(s"Check $filename failed. Please regenerate the ranger policy file by running"
        + "`KYUUBI_UPDATE=1 build/mvn clean test -Pgen-policy"
        + " -pl :kyuubi-spark-authz_2.12 -Dtest=none"
        + " -DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.gen.JsonSpecFileGenerator`.") {
        assert(generatedStr.equals(existedFileContent))
      }
    }
  }
}
