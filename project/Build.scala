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
import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8

import scala.io.Source
import scala.util.Properties

import com.simplytyped.Antlr4Plugin._
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import org.scalastyle.sbt.Tasks
import sbt._
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbt.librarymanagement.{SemanticSelector, VersionNumber}
import sbtpomreader.{PomBuild, SbtPomKeys}

object BuildCommons {
  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  val extensionsServer @ Seq(
  kyuubiServerPlugin
  ) = Seq(
    "kyuubi-server-plugin"
  ).map(ProjectRef(buildLocation, _))

  val extensionsSpark @ Seq(
  kyuubiExtensionSparkJdbcDialect,
  kyuubiSparkAuthz,
  kyuubiSparkAuthzShaded,
  kyuubiSparkConnectorCommon,
  kyuubiSparkConnectorHive,
  kyuubiSparkConnectorTpcds,
  kyuubiSparkConnectorTpch,
  kyuubiSparkLineage
  ) = Seq(
    "kyuubi-extension-spark-jdbc-dialect",
    "kyuubi-spark-authz",
    "kyuubi-spark-authz-shaded",
    "kyuubi-spark-connector-common",
    "kyuubi-spark-connector-hive",
    "kyuubi-spark-connector-tpcds",
    "kyuubi-spark-connector-tpch",
    "kyuubi-spark-lineage"
  ).map(ProjectRef(buildLocation, _))

  val externals @ Seq(
  kyuubiChatEngine,
  kyuubiDownload,
  kyuubiFlinkSqlEngine,
  kyuubiHiveSqlEngine,
  kyuubiJdbcEngine,
  kyuubiSparkSqlEngine,
  kyuubiTrinoEngine) = Seq(
    "kyuubi-chat-engine",
    "kyuubi-download",
    "kyuubi-flink-sql-engine",
    "kyuubi-hive-sql-engine",
    "kyuubi-jdbc-engine",
    "kyuubi-spark-sql-engine",
    "kyuubi-trino-engine").map(ProjectRef(buildLocation, _))

  val integrationTests @ Seq(
    kyuubiFlinkIt,
    kyuubiHiveIt,
    kyuubiJdbcIt,
    kyuubiKubernetesIt,
    kyuubiTrinoIt,
    kyuubiZookeeperIt
  ) = Seq(
    "kyuubi-flink-it",
    "kyuubi-hive-it",
    "kyuubi-jdbc-it",
    "kyuubi-kubernetes-it",
    "kyuubi-trino-it",
    "kyuubi-zookeeper-it"
  ).map(ProjectRef(buildLocation, _))

  val allProjects @ Seq(
  kyuubi,
  kyuubiAssembly,
  kyuubiCommon,
  kyuubiCtl,
  kyuubiEvents,
  kyuubiHa,
  kyuubiHiveBeeline,
  kyuubiHiveJdbc,
  kyuubiHiveJdbcShaded,
  kyuubiMetrics,
  kyuubiRestClient,
  kyuubiServer,
  kyuubiUtil,
  kyuubiUtilScala,
  kyuubiZookeeper,
  kyuubiExtensionSparkCommon,
  _*) = Seq(
    "kyuubi",
    "kyuubi-assembly",
    "kyuubi-common",
    "kyuubi-ctl",
    "kyuubi-events",
    "kyuubi-ha",
    "kyuubi-hive-beeline",
    "kyuubi-hive-jdbc",
    "kyuubi-hive-jdbc-shaded",
    "kyuubi-metrics",
    "kyuubi-rest-client",
    "kyuubi-server",
    "kyuubi-util",
    "kyuubi-util-scala",
    "kyuubi-zookeeper",
    "kyuubi-extension-spark-common")
    .map(ProjectRef(buildLocation, _)) ++ extensionsSpark ++ extensionsServer ++ externals ++ integrationTests
}

object KyuubiBuild extends PomBuild {
  import BuildCommons._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    if (profiles.contains("jwdp-test-debug")) {
      sys.props.put("test.jwdp.enabled", "true")
    }
    profiles
  }

  lazy val scalaStyleRules = Project("scalaStyleRules", file("scalastyle"))
    .settings(
      libraryDependencies += "org.scalastyle" %% "scalastyle" % "1.0.0")

  lazy val scalaStyleOnCompile = taskKey[Unit]("scalaStyleOnCompile")

  lazy val scalaStyleOnTest = taskKey[Unit]("scalaStyleOnTest")

  val scalaStyleOnCompileConfig: String = {
    val in = "scalastyle-config.xml"
    val out = "scalastyle-on-compile.generated.xml"
    val replacements = Map(
      """customId="println" level="error"""" -> """customId="println" level="warn"""")
    var contents = Source.fromFile(in).getLines.mkString("\n")
    for ((k, v) <- replacements) {
      require(contents.contains(k), s"Could not rewrite '$k' in original scalastyle config.")
      contents = contents.replace(k, v)
    }
    new PrintWriter(out) {
      write(contents)
      close()
    }
    out
  }

  // Return a cached scalastyle task for a given configuration (usually Compile or Test)
  private def cachedScalaStyle(config: Configuration) = Def.task {
    val logger = streams.value.log
    // We need a different cache dir per Configuration, otherwise they collide
    val cacheDir = target.value / s"scalastyle-cache-${config.name}"
    val cachedFun = FileFunction.cached(cacheDir, FilesInfo.lastModified, FilesInfo.exists) {
      (inFiles: Set[File]) =>
      {
        val args: Seq[String] = Seq.empty
        val scalaSourceV = Seq(file((config / scalaSource).value.getAbsolutePath))
        val configV = (ThisBuild / baseDirectory).value / scalaStyleOnCompileConfig
        val configUrlV = (config / scalastyleConfigUrl).value
        val streamsV = ((config / streams).value: @sbtUnchecked)
        val failOnErrorV = true
        val failOnWarningV = false
        val scalastyleTargetV = (config / scalastyleTarget).value
        val configRefreshHoursV = (config / scalastyleConfigRefreshHours).value
        val targetV = (config / target).value
        val configCacheFileV = (config / scalastyleConfigUrlCacheFile).value

        logger.info(s"Running scalastyle on ${name.value} in ${config.name}")
        Tasks.doScalastyle(
          args,
          configV,
          configUrlV,
          failOnErrorV,
          failOnWarningV,
          scalaSourceV,
          scalastyleTargetV,
          streamsV,
          configRefreshHoursV,
          targetV,
          configCacheFileV)

        Set.empty
      }
    }

    cachedFun(findFiles((config / scalaSource).value))
  }

  private def findFiles(file: File): Set[File] = if (file.isDirectory) {
    file.listFiles().toSet.flatMap(findFiles) + file
  } else {
    Set(file)
  }

  def enableScalaStyle: Seq[sbt.Def.Setting[_]] = Seq(
    scalaStyleOnCompile := cachedScalaStyle(Compile).value,
    scalaStyleOnTest := cachedScalaStyle(Test).value,
    (Compile / compile) := {
      scalaStyleOnCompile.value
      (Compile / compile).value
    },
    (Test / compile) := {
      scalaStyleOnTest.value
      (Test / compile).value
    })

  // Silencer: Scala compiler plugin for warning suppression
  // Aim: enable fatal warnings, but suppress ones related to using of deprecated APIs
  // depends on scala version:
  // <2.13.2 - silencer 1.7.12 and compiler settings to enable fatal warnings
  // 2.13.2+ - no silencer and configured warnings to achieve the same
  lazy val compilerWarningSettings: Seq[sbt.Def.Setting[_]] = Seq(
    libraryDependencies ++= {
      if (VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("<2.13.2"))) {
        val silencerVersion = "1.7.13"
        Seq(
          "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0",
          compilerPlugin(
            "com.github.ghik" % "silencer-plugin" % silencerVersion cross CrossVersion.full),
          "com.github.ghik" % "silencer-lib" % silencerVersion % Provided cross CrossVersion.full)
      } else {
        Seq.empty
      }
    })

  lazy val sharedSettings = compilerWarningSettings ++
    (if (sys.env.contains("NOLINT_ON_COMPILE")) Nil else enableScalaStyle) ++ Seq(
    (Compile / exportJars) := true,
    (Test / exportJars) := false,
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p =>
        new File(p).getParentFile().getAbsolutePath()
      })
      .map(file),
    publishMavenStyle := true,
    (Compile / javacOptions) ++= Seq(
      "-encoding",
      UTF_8.name()),
    SbtPomKeys.profiles := profiles)

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef): Unit = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  (allProjects).foreach(enable(sharedSettings))
  enable(KyuubiExtensionSparkCommon.settings)(kyuubiExtensionSparkCommon)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }
}

object KyuubiExtensionSparkCommon {
  import com.simplytyped.Antlr4Plugin
  import com.simplytyped.Antlr4Plugin.autoImport._

  lazy val settings = Antlr4Plugin.projectSettings ++ Seq(
    (Antlr4 / antlr4Version) := SbtPomKeys.effectivePom.value.getProperties.get("antlr4.version").asInstanceOf[String],
    (Antlr4 / antlr4PackageName) := Some("org.apache.kyuubi.sql"),
    (Antlr4 / antlr4GenListener) := true,
    (Antlr4 / antlr4GenVisitor) := true,
    (Antlr4 / antlr4TreatWarningsAsErrors) := true
  )
}
