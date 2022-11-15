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

package org.apache.kyuubi

import java.io._
import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{Properties, TimeZone, UUID}

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.commons.lang3.SystemUtils
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.ShutdownHookManager

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.internal.Tests.IS_TESTING

object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  def strToSeq(s: String, sp: String = ","): Seq[String] = {
    require(s != null)
    s.split(sp).map(_.trim).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    sys.props.toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    getPropertiesFile(KYUUBI_CONF_FILE_NAME, env)
  }

  def getPropertiesFile(fileName: String, env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map(d => new File(d + File.separator + fileName))
      .filter(_.exists())
      .orElse {
        Option(Utils.getContextOrKyuubiClassLoader.getResource(fileName)).map { url =>
          new File(url.getFile)
        }.filter(_.exists())
      }
  }

  def getPropertiesFromFile(file: Option[File]): Map[String, String] = {
    file.map { f =>
      info(s"Loading Kyuubi properties from ${f.getAbsolutePath}")
      try {
        val reader = new InputStreamReader(f.toURI.toURL.openStream(), StandardCharsets.UTF_8)
        try {
          val properties = new Properties()
          properties.load(reader)
          properties.stringPropertyNames().asScala.map { k =>
            (k, properties.getProperty(k).trim)
          }.toMap
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          throw new KyuubiException(
            s"Failed when loading Kyuubi properties from ${f.getAbsolutePath}",
            e)
      }
    }.getOrElse(Map.empty)
  }

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "kyuubi"): Path = {
    var error: Exception = null
    (0 until MAX_DIR_CREATION_ATTEMPTS).foreach { _ =>
      val candidate = Paths.get(root, s"$namePrefix-${UUID.randomUUID()}")
      try {
        val path = Files.createDirectories(candidate)
        return path
      } catch {
        case e: IOException => error = e
      }
    }
    throw new IOException(
      "Failed to create a temp directory (under " + root + ") after " + MAX_DIR_CREATION_ATTEMPTS +
        " attempts!",
      error)
  }

  def getAbsolutePathFromWork(pathStr: String, env: Map[String, String] = sys.env): Path = {
    val path = Paths.get(pathStr)
    if (path.isAbsolute) {
      path
    } else {
      val workDir = env.get("KYUUBI_WORK_DIR_ROOT") match {
        case Some(dir) => dir
        case _ => System.getProperty("user.dir")
      }
      Paths.get(workDir, pathStr)
    }
  }

  /**
   * Delete a directory recursively.
   */
  def deleteDirectoryRecursively(f: File): Boolean = {
    if (f.isDirectory) f.listFiles match {
      case files: Array[File] => files.foreach(deleteDirectoryRecursively)
      case _ =>
    }
    f.delete()
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      prefix: String = "kyuubi",
      root: String = System.getProperty("java.io.tmpdir")): Path = {
    val dir = createDirectory(root, prefix)
    dir.toFile.deleteOnExit()
    dir
  }

  def currentUser: String = UserGroupInformation.getCurrentUser.getShortUserName

  private val shortVersionRegex = """^(\d+\.\d+\.\d+)(.*)?$""".r

  /**
   * Given a Kyuubi/Spark/Hive version string, return the short version string.
   * E.g., for 3.0.0-SNAPSHOT, return '3.0.0'.
   */
  def shortVersion(version: String): String = {
    shortVersionRegex.findFirstMatchIn(version) match {
      case Some(m) => m.group(1)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$version' as a project" +
          s" version string, but it could not find the major/minor/maintenance version numbers.")
    }
  }

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows: Boolean = SystemUtils.IS_OS_WINDOWS

  /**
   * Indicates whether Kyuubi is currently running unit tests.
   */
  def isTesting: Boolean = {
    System.getProperty(IS_TESTING.key) != null
  }

  val DEFAULT_SHUTDOWN_PRIORITY = 100
  val SERVER_SHUTDOWN_PRIORITY = 75
  // The value follows org.apache.spark.util.ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY
  // Hooks need to be invoked before the SparkContext stopped shall use a higher priority.
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50
  val FLINK_ENGINE_SHUTDOWN_PRIORITY = 50
  val TRINO_ENGINE_SHUTDOWN_PRIORITY = 50
  val JDBC_ENGINE_SHUTDOWN_PRIORITY = 50

  /**
   * Add some operations that you want into ShutdownHook
   * @param hook
   * @param priority: 0~100
   */
  def addShutdownHook(hook: Runnable, priority: Int = DEFAULT_SHUTDOWN_PRIORITY): Unit = {
    ShutdownHookManager.get().addShutdownHook(hook, priority)
  }

  /**
   * This block of code is based on Spark's Utils.findLocalInetAddress()
   */
  def findLocalInetAddress: InetAddress = {
    val address = InetAddress.getLocalHost
    if (address.isLoopbackAddress) {
      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
      val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

      for (ni <- reOrderedNetworkIFs) {
        val addresses = ni.getInetAddresses.asScala
          .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
        if (addresses.nonEmpty) {
          val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
          // because of Inet6Address.toHostName may add interface at the end if it knows about it
          val strippedAddress = InetAddress.getByAddress(addr.getAddress)
          // We've found an address that looks reasonable!
          warn(s"${address.getHostName} was resolved to a loopback address: " +
            s"${address.getHostAddress}, using ${strippedAddress.getHostAddress}")
          return strippedAddress
        }
      }
      warn(s"${address.getHostName} was resolved to a loopback address: ${address.getHostAddress}" +
        " but we couldn't find any external IP address!")
    }
    address
  }

  /**
   * return date of format yyyyMMdd
   */
  def getDateFromTimestamp(time: Long): String = {
    DateFormatUtils.format(time, "yyyyMMdd", TimeZone.getDefault)
  }

  /**
   * Make a string representation of the exception.
   */
  def stringifyException(e: Throwable): String = {
    val stm = new StringWriter
    val wrt = new PrintWriter(stm)
    e.printStackTrace(wrt)
    wrt.close()
    stm.toString
  }

  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(t) =>
        error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  def getCodeSourceLocation(clazz: Class[_]): String = {
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
  }

  def fromCommandLineArgs(args: Array[String], conf: KyuubiConf): Unit = {
    require(args.length % 2 == 0, s"Illegal size of arguments.")
    for (i <- args.indices by 2) {
      require(
        args(i) == "--conf",
        s"Unrecognized main arguments prefix ${args(i)}," +
          s"the argument format is '--conf k=v'.")

      args(i + 1).split("=", 2).map(_.trim) match {
        case seq if seq.length == 2 => conf.set(seq.head, seq.last)
        case _ => throw new IllegalArgumentException(s"Illegal argument: ${args(i + 1)}.")
      }
    }
  }

  val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  private val PATTERN_FOR_KEY_VALUE_ARG = "(.+?)=(.+)".r

  def redactCommandLineArgs(conf: KyuubiConf, commands: Array[String]): Array[String] = {
    val redactionPattern = conf.get(SERVER_SECRET_REDACTION_PATTERN)
    var nextKV = false
    commands.map {
      case PATTERN_FOR_KEY_VALUE_ARG(key, value) if nextKV =>
        val (_, newValue) = redact(redactionPattern, Seq((key, value))).head
        nextKV = false
        s"$key=$newValue"

      case cmd if cmd == "--conf" =>
        nextKV = true
        cmd

      case cmd =>
        cmd
    }
  }

  /**
   * Redact the sensitive values in the given map. If a map key matches the redaction pattern then
   * its value is replaced with a dummy text.
   */
  def redact[K, V](regex: Option[Regex], kvs: Seq[(K, V)]): Seq[(K, V)] = {
    regex match {
      case None => kvs
      case Some(r) => redact(r, kvs)
    }
  }

  private def redact[K, V](redactionPattern: Regex, kvs: Seq[(K, V)]): Seq[(K, V)] = {
    kvs.map {
      case (key: String, value: String) =>
        redactionPattern.findFirstIn(key)
          .orElse(redactionPattern.findFirstIn(value))
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value: String) =>
        redactionPattern.findFirstIn(value)
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value) =>
        (key, value)
    }.asInstanceOf[Seq[(K, V)]]
  }

  def isCommandAvailable(cmd: String): Boolean = s"which $cmd".! == 0

  /**
   * Get the ClassLoader which loaded Kyuubi.
   */
  def getKyuubiClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Kyuubi.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrKyuubiClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getKyuubiClassLoader)

  def isOnK8s: Boolean = Files.exists(Paths.get("/var/run/secrets/kubernetes.io"))
}
