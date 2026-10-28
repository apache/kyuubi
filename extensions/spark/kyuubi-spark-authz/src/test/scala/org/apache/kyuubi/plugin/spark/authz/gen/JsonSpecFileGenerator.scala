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

import scala.collection.mutable
import scala.io.Source

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
    assertLedgerHasNoStaleEntries()
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

  private val verifiedVersionsLedgerFile = "spec_verified_spark_versions.txt"

  // Spark-version provenance for specs that do not declare verifiedSparkVersions at their
  // definition site. There is deliberately no default: a spec that is neither declared nor
  // listed fails generation, so a new spec cannot silently inherit the pre-Spark-4-port
  // baseline the way it could when this was a blanket back-fill. See the ledger's header.
  private lazy val verifiedVersionsLedger: Map[String, Seq[String]] = {
    val ledgerPath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/test/resources/$verifiedVersionsLedgerFile")
    val source = Source.fromFile(ledgerPath.toFile, StandardCharsets.UTF_8.name)
    val entries =
      try source.getLines().map(_.takeWhile(_ != '#').trim).filter(_.nonEmpty).toList
      finally source.close()
    entries.map { entry =>
      val fields = entry.split("\\s+").toSeq
      fields.head -> fields.tail
    }.toMap
  }

  private val ledgerEntriesUsed = mutable.Set.empty[String]

  private def withVerifiedVersions[T <: CommandSpec](spec: T): T = {
    if (spec.verifiedSparkVersions.nonEmpty) {
      return spec
    }
    ledgerEntriesUsed += spec.classname
    val versions = verifiedVersionsLedger.getOrElse(
      spec.classname,
      fail(
        s"${spec.classname} declares no verifiedSparkVersions and is absent from" +
          s" $verifiedVersionsLedgerFile. Set verifiedSparkVersions at the spec's" +
          " definition site to the exact Spark major.minor versions it was reviewed" +
          " against, using Seq.empty if none. Do not add it to the ledger: that file" +
          " records the pre-Spark-4-port baseline and is not meant to grow."))
    val populated: CommandSpec = spec match {
      case s: DatabaseCommandSpec => s.copy(verifiedSparkVersions = versions)
      case s: TableCommandSpec => s.copy(verifiedSparkVersions = versions)
      case s: FunctionCommandSpec => s.copy(verifiedSparkVersions = versions)
      case s: ScanSpec => s.copy(verifiedSparkVersions = versions)
      case s => s
    }
    populated.asInstanceOf[T]
  }

  // A ledger entry for a spec that no longer exists is dead weight that reads as
  // provenance, so retire it along with its spec.
  private def assertLedgerHasNoStaleEntries(): Unit = {
    val staleEntries = verifiedVersionsLedger.keySet -- ledgerEntriesUsed
    withClue(
      s"$verifiedVersionsLedgerFile has entries for specs that no longer take their" +
        s" versions from it, remove them: $staleEntries")(
      assertResult(Set.empty[String])(staleEntries))
  }

  def writeCommandSpecJson[T <: CommandSpec](
      commandType: String,
      specsArr: Seq[CommandSpecs[T]]): Unit = {
    val filename = s"${commandType}_command_spec.json"
    val filePath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/main/resources/$filename")

    val allSpecs = specsArr.flatMap(_.specs.sortBy(_.classname))
      .map(withVerifiedVersions)
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
