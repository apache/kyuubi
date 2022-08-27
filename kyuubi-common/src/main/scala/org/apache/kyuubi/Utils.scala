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
import java.util.{ArrayList => JArrayList, Properties, TimeZone, UUID}

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
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map(d => new File(d + File.separator + KYUUBI_CONF_FILE_NAME))
      .filter(_.exists())
      .orElse {
        Option(Utils.getContextOrKyuubiClassLoader.getResource(KYUUBI_CONF_FILE_NAME)).map { url =>
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
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "kyuubi"): Path = {
    val dir = createDirectory(root, namePrefix)
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

  /**
   * Get the query list that separated by SemiColon.
   * Copied from Apache Spark project SparkSQLCLIDriver::splitSemiColon.
   */
  private[hive] def splitQueriesBySemiColon(queries: String): Seq[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var isStatement = false
    val ret = new JArrayList[String]

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0
    def insideComment: Boolean = insideSimpleComment || insideBracketedComment
    def statementInProgress(index: Int): Boolean = isStatement || (!insideComment &&
      index > beginIndex && !s"${queries.charAt(index)}".trim.isEmpty)

    for (index <- 0 until queries.length) {
      // Checks if we need to decrement a bracketed comment level; the last character '/' of
      // bracketed comments is still inside the comment, so `insideBracketedComment` must keep true
      // in the previous loop and we decrement the level here if needed.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (queries.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (queries.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (queries.charAt(index) == '-') {
        val hasNext = index + 1 < queries.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && queries.charAt(index + 1) == '-') {
          // ignore quotes and ; in simple comment
          insideSimpleComment = true
        }
      } else if (queries.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // do not split
        } else {
          if (isStatement) {
            // split, do not include ; itself
            ret.add(queries.substring(beginIndex, index))
          }
          beginIndex = index + 1
          isStatement = false
        }
      } else if (queries.charAt(index) == '\n') {
        // with a new line the inline simple comment should end.
        if (!escape) {
          insideSimpleComment = false
        }
      } else if (queries.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < queries.length
        if (insideSingleQuote || insideDoubleQuote) {
          // Ignores '/' in any case of quotes
        } else if (insideBracketedComment && queries.charAt(index - 1) == '*') {
          // Decrements `bracketedCommentLevel` at the beginning of the next loop
          leavingBracketedComment = true
        } else if (hasNext && queries.charAt(index + 1) == '*') {
          bracketedCommentLevel += 1
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (queries.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }
    // Check the last char is end of nested bracketed comment.
    val endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1
    // Spark SQL support simple comment and nested bracketed comment in query body.
    // But if Spark SQL receives a comment alone, it will throw parser exception.
    // In Spark SQL CLI, if there is a completed comment in the end of whole query,
    // since Spark SQL CLL use `;` to split the query, CLI will pass the comment
    // to the backend engine and throw exception. CLI should ignore this comment,
    // If there is an uncompleted statement or an uncompleted bracketed comment in the end,
    // CLI should also pass this part to the backend engine, which may throw an exception
    // with clear error message.
    if (!endOfBracketedComment && (isStatement || insideBracketedComment)) {
      ret.add(queries.substring(beginIndex))
    }
    ret.asScala
  }
}
