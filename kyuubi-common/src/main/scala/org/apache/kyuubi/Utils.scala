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

import java.io.{File, InputStreamReader, IOException}
import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.ShutdownHookManager

import org.apache.kyuubi.config.internal.Tests.IS_TESTING

private[kyuubi] object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  def strToSeq(s: String): Seq[String] = {
    require(s != null)
    s.split(",").map(_.trim).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    sys.props.toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map( d => new File(d + File.separator + KYUUBI_CONF_FILE_NAME))
      .filter(_.exists())
      .orElse {
        Option(getClass.getClassLoader.getResource(KYUUBI_CONF_FILE_NAME)).map { url =>
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
            s"Failed when loading Kyuubi properties from ${f.getAbsolutePath}", e)
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
    throw new IOException("Failed to create a temp directory (under " + root + ") after " +
      MAX_DIR_CREATION_ATTEMPTS + " attempts!", error)
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "kyuubi"): Path = {
    val dir = createDirectory(root, namePrefix)
    dir.toFile.deleteOnExit()
    dir
  }

  def currentUser: String = UserGroupInformation.getCurrentUser.getShortUserName

  private val majorMinorRegex = """^(\d+)\.(\d+)(\..*)?$""".r
  private val shortVersionRegex = """^(\d+\.\d+\.\d+)(.*)?$""".r

  /**
   * Given a Kyuubi/Spark/Hive version string, return the major version number.
   * E.g., for 2.0.1-SNAPSHOT, return 2.
   */
  def majorVersion(version: String): Int = majorMinorVersion(version)._1

  /**
   * Given a Kyuubi/Spark/Hive version string, return the minor version number.
   * E.g., for 2.0.1-SNAPSHOT, return 0.
   */
  def minorVersion(version: String): Int = majorMinorVersion(version)._2

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
   * Given a Kyuubi/Spark/Hive version string,
   * return the (major version number, minor version number).
   * E.g., for 2.0.1-SNAPSHOT, return (2, 0).
   */
  def majorMinorVersion(version: String): (Int, Int) = {
    majorMinorRegex.findFirstMatchIn(version) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$version' as a project" +
          s" version string, but it could not find the major and minor version numbers.")
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
}
