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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

/**
 * Generates the default command specs to src/main/resources dir.
 *
 * To run the test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_ranger_spec_json.sh
 * }}}
 *
 * To regenerate the ranger policy file:
 * {{{
 *   dev/gen/gen_ranger_spec_json.sh
 * }}}
 */
class JsonSpecFileGenerator extends KyuubiFunSuite {
  test("check spec json files") {
    writeCommandSpecJson("database", Seq(DatabaseCommands))
    writeCommandSpecJson(
      "table",
      Seq(TableCommands, IcebergCommands, HudiCommands, DeltaCommands, PaimonCommands))
    writeCommandSpecJson("function", Seq(FunctionCommands))
    writeCommandSpecJson("scan", Seq(Scans))
    writeHarmlessNodeSpecJson()
  }

  def writeHarmlessNodeSpecJson(): Unit = {
    val filename = "known_harmless_spec.json"
    val filePath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/main/resources/$filename")

    val allSpecs = KnownHarmlessNodes.specs.sortBy(_.classname)
    val duplicatedClassnames = allSpecs.groupBy(_.classname).values
      .filter(_.size > 1).flatMap(specs => specs.map(_.classname)).toSet
    withClue(s"Unexpected duplicated classnames: $duplicatedClassnames")(
      assertResult(0)(duplicatedClassnames.size))
    val generatedStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(allSpecs)

    if (sys.env.get("KYUUBI_UPDATE").contains("1")) {
      // scalastyle:off println
      println(s"writing ${allSpecs.length} specs to $filename")
      // scalastyle:on println
      Files.write(
        filePath,
        generatedStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(
        filePath,
        Seq(generatedStr),
        "dev/gen/gen_ranger_spec_json.sh",
        splitFirstExpectedLine = true)
    }
  }

  // Every entry currently in the spec files predates the Spark 4 port, so this is the
  // audited baseline for any spec that doesn't declare its own verified versions. Specs
  // verified on other Spark minors should set verifiedSparkVersions explicitly at their
  // definition site. Note the field is advisory for command/scan specs (they still engage
  // on unaudited versions), unlike the allowlist where it gates.
  private val defaultVerifiedSparkVersions = Seq("3.3", "3.4", "3.5")

  private def withDefaultVerifiedVersions[T <: CommandSpec](spec: T): T = {
    val populated: CommandSpec = spec match {
      case s: DatabaseCommandSpec if s.verifiedSparkVersions.isEmpty =>
        s.copy(verifiedSparkVersions = defaultVerifiedSparkVersions)
      case s: TableCommandSpec if s.verifiedSparkVersions.isEmpty =>
        s.copy(verifiedSparkVersions = defaultVerifiedSparkVersions)
      case s: FunctionCommandSpec if s.verifiedSparkVersions.isEmpty =>
        s.copy(verifiedSparkVersions = defaultVerifiedSparkVersions)
      case s: ScanSpec if s.verifiedSparkVersions.isEmpty =>
        s.copy(verifiedSparkVersions = defaultVerifiedSparkVersions)
      case s => s
    }
    populated.asInstanceOf[T]
  }

  def writeCommandSpecJson[T <: CommandSpec](
      commandType: String,
      specsArr: Seq[CommandSpecs[T]]): Unit = {
    val filename = s"${commandType}_command_spec.json"
    val filePath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/main/resources/$filename")

    val allSpecs = specsArr.flatMap(_.specs.sortBy(_.classname))
      .map(withDefaultVerifiedVersions)
    val duplicatedClassnames = allSpecs.groupBy(_.classname).values
      .filter(_.size > 1).flatMap(specs => specs.map(_.classname)).toSet
    withClue(s"Unexpected duplicated classnames: $duplicatedClassnames")(
      assertResult(0)(duplicatedClassnames.size))
    val generatedStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(allSpecs)

    if (sys.env.get("KYUUBI_UPDATE").contains("1")) {
      // scalastyle:off println
      println(s"writing ${allSpecs.length} specs to $filename")
      // scalastyle:on println
      Files.write(
        filePath,
        generatedStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(
        filePath,
        Seq(generatedStr),
        "dev/gen/gen_ranger_spec_json.sh",
        splitFirstExpectedLine = true)
    }
  }
}
