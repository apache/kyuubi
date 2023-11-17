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

import java.util.Locale

import sbt._
import sbt.Keys._

import sbtpomreader.{PomBuild, SbtPomKeys}
import sbtprotoc.ProtocPlugin.autoImport._
import sbtassembly.AssemblyPlugin.autoImport._

import scala.util.Properties


object KyuubiBuild extends PomBuild {

  import scala.collection.mutable.Map

  private val buildLocation = file(".").getAbsoluteFile.getParentFile

  override val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
      case None => Seq("sbt")
      case Some(v) =>
        v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
    }
    if (profiles.contains("jdwp-test-debug")) {
      sys.props.put("test.jdwp.enabled", "true")
    }
    profiles
  }

  val kyuubiCommon = ProjectRef(buildLocation, "kyuubi-common")
  val kyuubiServer = ProjectRef(buildLocation, "kyuubi-server")


  val spark3Client = ProjectRef(buildLocation, "spark-3-shaded")

  val allProjects@Seq(
  client, server, ha, _*
  ) = Seq(
    "kyuubi-rest-client", "kyuubi-servers", "worker"
  ).map(ProjectRef(buildLocation, _)).++(Seq(kyuubiCommon, kyuubiServer, spark3Client))


  lazy val sharedSettings = Seq(
    SbtPomKeys.profiles.:=(profiles)
  )

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef): Any = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap.+=((projectRef.project -> (existingSettings ++ settings)))
  }

  enable(KyuubiCommon.settings)(kyuubiCommon)
  enable(KyuubiMaster.settings)(kyuubiServer)
  enable(KyuubiSpark3Client.settings)(spark3Client)

  allProjects.foreach(enable(sharedSettings))

  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    return super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1.==(x.id))) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }
}


object KyuubiCommon {
  val protocVersion = "3.19.2"
  val protoVersion = "3.19.2"
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protocVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    }).++ {
    Seq(
      Compile / PB.protoSources := Seq(
        sourceDirectory.value / "main" / "proto"
      ),
      (Compile / PB.targets) := Seq(
        PB.gens.java -> (Compile / sourceManaged).value
        // PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value
      )
    )
  }
}

object KyuubiMaster {
  val protocVersion = "3.19.2"
  val protoVersion = "3.19.2"
  lazy val settings = Seq(
    // Setting version for the protobuf compiler. This has to be propagated to every sub-project
    // even if the project is not using it.
    PB.protocVersion := protocVersion,
    // For some reason the resolution from the imported Maven build does not work for some
    // of these dependendencies that we need to shade later on.
    libraryDependencies ++= {
      Seq(
        "com.google.protobuf" % "protobuf-java" % protoVersion % "protobuf"
      )
    }) ++ {
    Seq(
      Compile / PB.protoSources := Seq(
        sourceDirectory.value / "main" / "proto"
      ),
      (Compile / PB.targets) := Seq(
        PB.gens.java -> (Compile / sourceManaged).value
        // PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value
      )
    )
  }
}

object KyuubiSpark3Client {
  lazy val settings = Seq(
    (assembly / test) := { },

    (assembly / logLevel) := Level.Info,

    // Exclude `scala-library` from assembly.
    (assembly / assemblyPackageScala / assembleArtifact) := false,

    // Exclude `pmml-model-*.jar`, `scala-collection-compat_*.jar`,`jsr305-*.jar` and
    // `netty-*.jar` and `unused-1.0.0.jar` from assembly.
    (assembly / assemblyExcludedJars) := {
      val cp = (assembly / fullClasspath).value
      cp filter { v =>
        val name = v.data.getName
        // name.startsWith("pmml-model-") || name.startsWith("scala-collection-compat_") ||
        //  name.startsWith("jsr305-") || name.startsWith("netty-") || name == "unused-1.0.0.jar"
        !(name.startsWith("celeborn-") || name.startsWith("protobuf-java-") ||
          name.startsWith("guava-") || name.startsWith("netty-") || name.startsWith("commons-lang3-"))
      }
    },

    (assembly / assemblyShadeRules) := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
      ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
      ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll
    ),

    (assembly / assemblyMergeStrategy) := {
      case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
      // Drop all proto files that are not needed as artifacts of the build.
      case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
      case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
      // Drop netty jnilib
      case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
      // rename netty native lib
      case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so" )
      case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so" )
      case _ => MergeStrategy.first
    }
  )
}
