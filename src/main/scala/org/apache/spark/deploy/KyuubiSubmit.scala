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
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.Properties

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

/**
 * Kyuubi version of SparkSubmit
 */
object KyuubiSubmit {

  // Cluster managers
  private val YARN = 1

  // Deploy modes
  private val CLIENT = 1
  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // scalastyle:off println
  // Exposed for testing
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
    printStream.println("Branch %s".format(SPARK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(SPARK_BUILD_USER, SPARK_BUILD_DATE))
    printStream.println("Revision %s".format(SPARK_REVISION))
    printStream.println("Url %s".format(SPARK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }
  // scalastyle:on println

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    submit(appArgs)
  }

  /**
   * Submit the application using the provided parameters.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   */
  private def submit(args: SparkSubmitArguments): Unit = {
    val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)
    runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
  }

  /**
   * Prepare the environment for submitting an application.
   * This returns a 4-tuple:
   *   (1) the arguments for the child process,
   *   (2) a list of classpath entries for the child,
   *   (3) a map of system properties, and
   *   (4) the main class for the child
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments)
  : (Seq[String], Seq[String], Map[String, String], String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    val childMainClass = args.mainClass

    args.master match {
      case "yarn" =>
      case "yarn-client" =>
        printWarning(s"Master ${args.master} is deprecated since 2.0." +
          " Please use master \"yarn\" with specified deploy mode instead.")
        args.master = "yarn"
      case _ => printErrorAndExit("Kyuubi only supports yarn as master.")
    }

    args.deployMode match {
      case "client" =>
      case _ => printWarning("Kyuubi only supports client mode.")
        args.deployMode = "client"

    }

    // Make sure YARN is included in our build if we're trying to use it
    if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client")) {
      printErrorAndExit(
        "Could not load YARN classes. Spark may not have been compiled with YARN support.")
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    Seq[OptionAssigner](
      OptionAssigner(args.master, YARN, CLIENT, sysProp = "spark.master"),
      OptionAssigner(args.deployMode, YARN, CLIENT, sysProp = "spark.submit.deployMode"),
      OptionAssigner(args.name, YARN, CLIENT, sysProp = "spark.app.name"),
      OptionAssigner(args.driverMemory, YARN, CLIENT, sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, YARN, CLIENT,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, YARN, CLIENT,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, YARN, CLIENT,
        sysProp = "spark.driver.extraLibraryPath"),
      OptionAssigner(args.queue, YARN, CLIENT, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, CLIENT,
        sysProp = "spark.executor.instances"),
      OptionAssigner(args.jars, YARN, CLIENT, sysProp = "spark.yarn.dist.jars"),
      OptionAssigner(args.files, YARN, CLIENT, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, CLIENT, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(args.principal, YARN, CLIENT, sysProp = "spark.yarn.principal"),
      OptionAssigner(args.keytab, YARN, CLIENT, sysProp = "spark.yarn.keytab"),
      OptionAssigner(args.executorCores, YARN, CLIENT, sysProp = "spark.executor.cores"),
      OptionAssigner(args.executorMemory, YARN, CLIENT, sysProp = "spark.executor.memory")
    ).foreach(opt => sysProps.put(opt.sysProp, opt.value))

    childClasspath += args.primaryResource
    if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    if (args.childArgs != null) { childArgs ++= args.childArgs }
    val jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty) ++
      Seq(args.primaryResource)
    sysProps.put("spark.jars", jars.mkString(","))

    if (args.principal != null) {
      require(args.keytab != null, "Keytab must be specified when principal is specified")
      if (!new File(args.keytab).exists()) {
        throw new SparkException(s"Keytab file: ${args.keytab} does not exist")
      } else {
        // Add keytab and principal configurations in sysProps to make them available
        // for later use; e.g. in spark sql, the isolated class loader used to talk
        // to HiveMetastore will use these settings. They will be set as Java system
        // properties and then loaded by SparkConf
        sysProps.put("spark.yarn.keytab", args.keytab)
        sysProps.put("spark.yarn.principal", args.principal)

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

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  private def runMain(
      childArgs: Seq[String],
      childClasspath: Seq[String],
      sysProps: Map[String, String],
      childMainClass: String,
      verbose: Boolean): Unit = {
    // scalastyle:off println
    if (verbose) {
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      // sysProps may contain sensitive information, so redact before printing
      printStream.println(s"System properties:\n${Utils.redact(sysProps).mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }
    // scalastyle:on println

    val url = this.getClass.getProtectionDomain.getCodeSource.getLocation
    val loader = new ChildFirstURLClassLoader(Array(url),
      Thread.currentThread.getContextClassLoader)

    Thread.currentThread.setContextClassLoader(loader)

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName(childMainClass)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    @tailrec
    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause != null) findCause(e.getCause) else e
      case e: InvocationTargetException =>
        if (e.getCause != null) findCause(e.getCause) else e
      case e: Throwable =>
        e
    }

    try {
      mainMethod.invoke(null, childArgs.toArray)
    } catch {
      case t: Throwable =>
        findCause(t) match {
          case SparkUserAppException(exitCode) =>
            System.exit(exitCode)
          case t: Throwable =>
            throw t
        }
    }
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
