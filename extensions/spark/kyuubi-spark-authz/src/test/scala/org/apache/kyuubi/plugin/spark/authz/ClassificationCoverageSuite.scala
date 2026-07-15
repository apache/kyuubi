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

package org.apache.kyuubi.plugin.spark.authz

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.jar.JarFile

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical.{Command, LeafNode, LogicalPlan}
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.SPARK_RUNTIME_MAJOR_MINOR
import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

/**
 * Build-time (per Spark profile) coverage checks over the spec files. These catch
 * classification drift when bumping Spark versions at PR time instead of at customer
 * runtime. They complement, not replace, the runtime checks in [[ParanoidMode]]: this
 * suite can only see classes on the build classpath, while third-party catalog plugins
 * appear only in the user's environment.
 */
class ClassificationCoverageSuite extends AnyFunSuite {
  // scalastyle:on

  private def loadable(classname: String): Option[Class[_]] = {
    try {
      Some(Class.forName(classname, false, getClass.getClassLoader))
    } catch {
      // not every spec'd class exists on every Spark/catalog-plugin profile
      case _: ClassNotFoundException | _: NoClassDefFoundError => None
    }
  }

  private lazy val executableDuringAnalysisClass: Option[Class[_]] =
    loadable("org.apache.spark.sql.catalyst.plans.logical.ExecutableDuringAnalysis")

  private val allCommandSpecClassnames: Set[String] =
    TABLE_COMMAND_SPECS.keySet ++ DB_COMMAND_SPECS.keySet ++ FUNCTION_COMMAND_SPECS.keySet

  test("every command spec entry present on this classpath is routable to buildCommand") {
    // A spec whose class name matches a plan node that the dispatch never routes to
    // buildCommand gives the appearance of coverage while enforcing nothing — the
    // CALL-on-Spark-4 shape: same fully-qualified name, different supertype.
    // Dispatch routes Commands and, as a fallback, any class with a command spec, so
    // plain reachability holds by construction; what can still silently break is a
    // spec'd node that executes *during analysis*, before any authorization rule runs.
    // Known, tracked gaps. An entry here is an ACKNOWLEDGED VULNERABILITY on the affected
    // Spark version, not a pass — it only keeps the build green while a fix is pending.
    // Do not add to this list without a plan to close the gap (an analysis-time check rule,
    // or blocking the operation outright on that Spark version).
    val acknowledgedGaps = Set(
      // On Spark 4.x, CALL is an ExecutableDuringAnalysis UnaryNode under the same class
      // name Iceberg used for its Spark 3 Command: the stored procedure runs during
      // analysis, before any authorization rule. See the paranoid-mode design doc, §2.
      "org.apache.spark.sql.catalyst.plans.logical.Call")

    val tooLateToAuthorize = allCommandSpecClassnames.toSeq.sorted.flatMap { name =>
      loadable(name).flatMap { cls =>
        executableDuringAnalysisClass match {
          case Some(eda) if eda.isAssignableFrom(cls) && !classOf[Command].isAssignableFrom(cls) =>
            Some(name)
          case _ => None
        }
      }
    }
    assert(
      tooLateToAuthorize.forall(acknowledgedGaps.contains),
      s"\nThese spec'd plan nodes execute during analysis, before RuleAuthorization runs," +
        s" so their command specs cannot enforce anything on this Spark version:" +
        s"\n  ${tooLateToAuthorize.filterNot(acknowledgedGaps.contains).mkString("\n  ")}\n" +
        s"Authorization for them must happen in an earlier (analysis-time) rule," +
        s" or the operation must be blocked outright on this Spark version.")
  }

  test("allowlisted classes present on this classpath are not Commands in disguise") {
    // A node allowlisted as harmless could later gain authz-relevant behavior; catching
    // an allowlist entry that is (or became) a Command forces a re-review on version bumps.
    val suspicious = KNOWN_HARMLESS_NODES.keySet.toSeq.sorted.flatMap { name =>
      loadable(name).flatMap { cls =>
        // Allowlisting a Command is higher-stakes than allowlisting a leaf relation, so it
        // takes a second, colocated review: the entry must also be exempted here.
        val exempted = Set(
          // mutate session conf only; sensitive configs guarded by AuthzConfigurationChecker
          "org.apache.spark.sql.execution.command.SetCommand",
          "org.apache.spark.sql.execution.command.ResetCommand",
          // row-filtered by ObjectFilterPlaceHolder + FilterDataSourceV2Strategy
          "org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces",
          "org.apache.spark.sql.catalyst.plans.logical.ShowTables",
          // session-local temp views are deliberately not authz resources
          "org.apache.spark.sql.execution.command.DropTempViewCommand",
          // resolves to literally nothing to execute
          "org.apache.spark.sql.catalyst.plans.logical.NoopCommand")
        if (classOf[Command].isAssignableFrom(cls) && !exempted.contains(name)) {
          Some(name)
        } else {
          None
        }
      }
    }
    assert(
      suspicious.isEmpty,
      s"""
         |These allowlisted 'harmless' classes are Commands on this Spark version;
         | re-review their known_harmless_spec.json entries:
         |  ${suspicious.mkString("\n  ")}""".stripMargin)
  }

  // ---------------------------------------------------------------------------------------
  // Enumeration check: diff the classpath's plan-node population against our classification.
  // ---------------------------------------------------------------------------------------

  /** Scan prefixes covering Spark itself and the catalog plugins that inject plan nodes. */
  private val scannedPackagePrefixes = Seq(
    "org/apache/spark/sql/",
    "org/apache/paimon/spark/")

  /** Classes whose presence identifies a jar that can contribute logical plan nodes. */
  private val jarAnchorClassnames = Seq(
    "org.apache.spark.sql.catalyst.plans.logical.LogicalPlan", // spark-catalyst
    "org.apache.spark.sql.execution.SparkPlan", // spark-sql core
    "org.apache.spark.sql.hive.HiveSessionStateBuilder", // spark-hive
    "org.apache.iceberg.spark.SparkCatalog", // iceberg runtime, if on this profile
    "org.apache.spark.sql.delta.DeltaLog", // delta, if on this profile
    "org.apache.spark.sql.hudi.command.CallProcedureHoodieCommand", // hudi, if on this profile
    "org.apache.paimon.spark.SparkCatalog" // paimon, if on this profile
  )

  /**
   * Concrete classes on this profile's classpath that carry authorization relevance by
   * shape: every Command, every LeafNode, and everything that executes during analysis.
   * These are exactly the shapes the runtime invariant refuses to let pass silently.
   */
  private def enumerateRelevantPlanClasses(): Seq[String] = {
    val loader = getClass.getClassLoader
    val jars = jarAnchorClassnames.flatMap(loadable).flatMap { cls =>
      Option(cls.getProtectionDomain.getCodeSource).map(_.getLocation)
    }.distinct.filter(_.getPath.endsWith(".jar"))

    val relevantSupertypes: Seq[Class[_]] =
      Seq(classOf[Command], classOf[LeafNode]) ++ executableDuringAnalysisClass

    jars.flatMap { jarUrl =>
      val jar = new JarFile(Paths.get(jarUrl.toURI).toFile)
      try {
        jar.entries().asScala
          .map(_.getName)
          .filter(n => n.endsWith(".class") && scannedPackagePrefixes.exists(n.startsWith))
          // skip anonymous/synthetic classes; keep nested ones (plan nodes can be nested)
          .filterNot(n => n.contains("$$") || n.matches(".*\\$\\d.*"))
          .map(_.stripSuffix(".class").replace('/', '.'))
          .flatMap { classname =>
            try {
              val cls = Class.forName(classname, false, loader)
              val concrete =
                !cls.isInterface && !java.lang.reflect.Modifier.isAbstract(cls.getModifiers)
              if (concrete &&
                classOf[LogicalPlan].isAssignableFrom(cls) &&
                relevantSupertypes.exists(_.isAssignableFrom(cls))) {
                Some(classname)
              } else {
                None
              }
            } catch {
              // optional dependencies of scanned classes may be absent at test time
              case _: Throwable => None
            }
          }.toList // strict: the iterator must be exhausted before the jar closes
      } finally {
        jar.close()
      }
    }.distinct.sorted
  }

  test("enumerate authz-relevant plan classes and diff against the classification") {
    // Golden backlog file, one classname per line, per Spark minor version. The contract:
    // a class NEW to this diff fails the build — classify it (command/scan spec), allowlist
    // it with a reason, or consciously add it to the backlog via regeneration. A class that
    // leaves the diff must also leave the backlog, so the backlog only ever shrinks by
    // being triaged, never silently.
    val backlogFilename = s"classification_backlog_spark_$SPARK_RUNTIME_MAJOR_MINOR.txt"
    val backlogPath = Paths.get(
      s"${getCurrentModuleHome(this)}/src/test/resources/$backlogFilename")

    val classified: Set[String] = allCommandSpecClassnames ++
      // an allowlist entry only classifies on the Spark minors it was reviewed against;
      // on this profile the others belong in the backlog awaiting re-review
      KNOWN_HARMLESS_NODES.filter(_._2.appliesTo(SPARK_RUNTIME_MAJOR_MINOR)).keySet ++
      // matched by nodeName rather than classname in buildQuery
      Set("org.apache.spark.sql.catalyst.analysis.UnresolvedRelation") ++
      SCAN_SPEC_CLASSNAMES

    val unclassified = enumerateRelevantPlanClasses().filterNot(classified)
    val generatedStr = unclassified.mkString("", "\n", "\n")

    if (sys.env.get("KYUUBI_UPDATE").contains("1")) {
      // scalastyle:off println
      println(s"writing ${unclassified.size} classnames to $backlogFilename")
      // scalastyle:on println
      Files.write(
        backlogPath,
        generatedStr.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      withClue(
        s"The set of unclassified authz-relevant plan classes on this classpath changed." +
          s" For every NEW class: add a command/scan spec, or an entry in" +
          s" known_harmless_spec.json with a reason; only leave it in the backlog as a" +
          s" conscious decision. Regenerate with KYUUBI_UPDATE=1 (dev/gen/gen_ranger_spec_json.sh" +
          s" regenerates the spec files; rerun this suite for the backlog).") {
        assertFileContent(
          backlogPath,
          Seq(generatedStr),
          "KYUUBI_UPDATE=1 build/mvn test -pl extensions/spark/kyuubi-spark-authz" +
            " -DwildcardSuites=org.apache.kyuubi.plugin.spark.authz.ClassificationCoverageSuite",
          splitFirstExpectedLine = true)
      }
    }
  }

  test("no class has both a command spec and an allowlist entry") {
    val both = allCommandSpecClassnames.intersect(KNOWN_HARMLESS_NODES.keySet)
    assert(
      both.isEmpty,
      s"""
         |Contradictory classification: both spec'd and allowlisted:
         |  ${both.toSeq.sorted.mkString("\n  ")}""".stripMargin)
  }
}
