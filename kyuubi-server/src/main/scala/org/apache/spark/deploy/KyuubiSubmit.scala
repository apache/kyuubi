/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy

import java.io.{File, PrintStream}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.util.{MutableURLClassLoader, Utils}

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.yarn.KyuubiYarnClient

/**
 * Kyuubi version of SparkSubmit
 */
object KyuubiSubmit extends Logging {

  // scalastyle:off println
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
  // scalastyle:on println

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    val (childClasspath, sysProps) = prepareSubmitEnvironment(appArgs)
    if (appArgs.verbose) {
      // scalastyle:off println
      // sysProps may contain sensitive information, so redact before printing
      printStream.println(s"System properties:\n${Utils.redact(sysProps).mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }
    // scalastyle:on println

    val loader = KyuubiSparkUtil.getAndSetKyuubiFirstClassLoader

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    try {
      if (appArgs.deployMode == "cluster") {
        KyuubiYarnClient.startKyuubiAppMaster()
      } else {
        KyuubiServer.startKyuubiServer()
      }
    } catch {
      case t: Throwable =>
        printErrorAndExit(s"Starting Kyuubi by ${appArgs.deployMode} " + t)
    }
  }

  /**
   * Prepare the environment for submitting an application.
   */
  private[deploy]
  def prepareSubmitEnvironment(args: SparkSubmitArguments): (Seq[String], Map[String, String]) = {
    // Make sure YARN is included in our build if we're trying to use it
    def checkYarnSupport: Unit = {
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client")) {
        printErrorAndExit(
          "Could not load YARN classes. Spark may not have been compiled with YARN support.")
      }
    }

    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()

    args.master match {
      case "yarn" => checkYarnSupport
      case "yarn-client" | "yarn-cluster" =>
        checkYarnSupport
        printWarning(s"Master ${args.master} is deprecated since 2.0." +
          " Please use master \"yarn\" with specified deploy mode instead.")
        args.master = "yarn"
      case m if m.startsWith("local") => args.master = "local"
      case _ => printErrorAndExit("Kyuubi only supports yarn, local as master.")
    }

    args.deployMode match {
      case "client" =>
      case "cluster" =>
      case _ => printWarning("Kyuubi only supports client mode.")
        args.deployMode = "client"

    }

    Seq(
      "spark.master" ->  args.master,
      "spark.submit.deployMode" -> args.deployMode,
      "spark.app.name" -> args.name,
      KyuubiSparkUtil.DRIVER_MEM -> args.driverMemory,
      "spark.driver.extraClassPath" -> args.driverExtraClassPath,
      KyuubiSparkUtil.DRIVER_EXTRA_JAVA_OPTIONS -> args.driverExtraJavaOptions,
      "spark.driver.extraLibraryPath" -> args.driverExtraLibraryPath,
      KyuubiSparkUtil.QUEUE -> args.queue,
      "spark.executor.instances" -> args.numExecutors,
      "spark.yarn.dist.jars" -> args.jars,
      "spark.yarn.dist.files" -> args.files,
      "spark.yarn.dist.archives" -> args.archives,
      KyuubiSparkUtil.PRINCIPAL -> args.principal,
      KyuubiSparkUtil.KEYTAB -> args.keytab,
      "spark.executor.cores" -> args.executorCores,
      "spark.executor.memory" -> args.executorMemory
    ).filter(_._2 != null).foreach(o => sysProps.put(o._1, o._2))

    if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    if (args.principal != null) {
      require(args.keytab != null, "Keytab must be specified when principal is specified")
      if (!new File(args.keytab).exists()) {
        throw new SparkException(s"Keytab file: ${args.keytab} does not exist")
      } else {
        // Add keytab and principal configurations in sysProps to make them available
        // for later use; e.g. in spark sql, the isolated class loader used to talk
        // to HiveMetastore will use these settings. They will be set as Java system
        // properties and then loaded by SparkConf
        sysProps.put(KyuubiSparkUtil.KEYTAB, args.keytab)
        sysProps.put(KyuubiSparkUtil.PRINCIPAL, args.principal)

        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sysProps.get(config).foreach { oldValue =>
        sysProps(config) = Utils.resolveURIs(oldValue)
      }
    }

    (childClasspath, sysProps)
  }

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        printWarning(s"Skip remote jar $uri.")
    }
  }
}
