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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.security.PrivilegedAction
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.ShutdownHookManager

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.internal.Tests.IS_TESTING
import org.apache.kyuubi.util.{JavaUtils, TempFileCleanupUtils}
import org.apache.kyuubi.util.command.CommandLineUtils._

object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  /**
   * An atomic counter used in writeToTempFile method
   * avoiding duplication in temporary file name generation
   */
  private lazy val tempFileIdCounter: AtomicLong = new AtomicLong(0)

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
      .orElse(env.get(KYUUBI_HOME_ENV_VAR_NAME).map(_ + File.separator + "conf"))
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

  def substituteKyuubiEnvVars(original: String, env: Map[String, String] = sys.env): String = {
    lazy val KYUUBI_HOME = env.getOrElse(
      KYUUBI_HOME_ENV_VAR_NAME,
      JavaUtils.getCodeSourceLocation(this.getClass).split("kyuubi-common").head)
    lazy val KYUUBI_WORK_DIR_ROOT = env.getOrElse(
      "KYUUBI_WORK_DIR_ROOT",
      Paths.get(KYUUBI_HOME, "work").toAbsolutePath.toString)
    // save cost of evaluating replacement when pattern not found
    def substitute(input: String, pattern: String, replacement: => String): String = {
      if (input.contains(pattern)) input.replace(pattern, replacement) else input
    }
    var substituted = original
    substituted = substitute(original, "<KYUUBI_HOME>", KYUUBI_HOME) // deprecated since 1.12.0
    substituted = substitute(substituted, "{{KYUUBI_HOME}}", KYUUBI_HOME)
    substituted = substitute(substituted, "{{KYUUBI_WORK_DIR_ROOT}}", KYUUBI_WORK_DIR_ROOT)
    substituted
  }

  /**
   * Delete a directory recursively.
   */
  def deleteDirectoryRecursively(f: File, ignoreException: Boolean = true): Unit = {
    if (f == null || !f.exists()) {
      return
    }

    if (f.isDirectory) {
      val files = f.listFiles
      if (files != null && files.nonEmpty) {
        files.foreach(deleteDirectoryRecursively(_, ignoreException))
      }
    }
    try {
      f.delete()
    } catch {
      case e: Exception =>
        if (ignoreException) {
          warn(s"Ignoring the exception in deleting file, path: ${f.toPath}", e)
        } else {
          throw e
        }
    }
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      prefix: String = "kyuubi",
      root: String = System.getProperty("java.io.tmpdir")): Path = {
    val dir = createDirectory(root, prefix)
    TempFileCleanupUtils.deleteOnExit(dir)
    dir
  }

  /**
   * List the files recursively in a directory.
   */
  def listFilesRecursively(file: File): Seq[File] = {
    if (!file.isDirectory) {
      file :: Nil
    } else {
      file.listFiles().flatMap(listFilesRecursively)
    }
  }

  /**
   * Copies bytes from an InputStream source to a newly created temporary file
   * created in the directory destination. The temporary file will be created
   * with new name by adding random identifiers before original file name's suffix,
   * and the file will be deleted on JVM exit. The directories up to destination
   * will be created if they don't already exist. destination will be overwritten
   * if it already exists. The source stream is closed.
   * @param source the InputStream to copy bytes from, must not be null, will be closed
   * @param dir the directory path for temp file creation
   * @param fileName original file name with suffix
   * @return the created temp file in dir
   */
  def writeToTempFile(source: InputStream, dir: Path, fileName: String): File = {
    try {
      if (source == null) {
        throw new IOException("the source inputstream is null")
      }
      if (!dir.toFile.exists()) {
        dir.toFile.mkdirs()
      }
      val (prefix, suffix) = fileName.lastIndexOf(".") match {
        case i if i > 0 => (fileName.substring(0, i), fileName.substring(i))
        case _ => (fileName, "")
      }
      val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
      val identifier = s"$currentTime-${tempFileIdCounter.incrementAndGet()}"
      val filePath = Paths.get(dir.toString, s"$prefix-$identifier$suffix")
      try {
        Files.copy(source, filePath, StandardCopyOption.REPLACE_EXISTING)
      } finally {
        source.close()
      }
      TempFileCleanupUtils.deleteOnExit(filePath)
      filePath.toFile
    } catch {
      case e: Exception =>
        error(
          s"failed to write to temp file in path $dir, original file name: $fileName",
          e)
        throw e
    }
  }

  def currentUser: String = UserGroupInformation.getCurrentUser.getShortUserName

  def doAs[T](
      proxyUser: String,
      realUser: UserGroupInformation = UserGroupInformation.getCurrentUser)(f: () => T): T = {
    UserGroupInformation.createProxyUser(proxyUser, realUser).doAs(new PrivilegedAction[T] {
      override def run(): T = f()
    })
  }

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

  def fromCommandLineArgs(args: Array[String], conf: KyuubiConf): Unit = {
    require(args.length % 2 == 0, s"Illegal size of arguments.")
    for (i <- args.indices by 2) {
      require(
        args(i) == CONF,
        s"Unrecognized main arguments prefix ${args(i)}," +
          s"the argument format is '--conf k=v'.")

      args(i + 1).split("=", 2).map(_.trim) match {
        case seq if seq.length == 2 => conf.set(seq.head, seq.last)
        case _ => throw new IllegalArgumentException(s"Illegal argument: ${args(i + 1)}.")
      }
    }
  }

  def redactCommandLineArgs(conf: KyuubiConf, commands: Iterable[String]): Iterable[String] = {
    conf.get(SERVER_SECRET_REDACTION_PATTERN) match {
      case Some(redactionPattern) =>
        var nextKV = false
        commands.map {
          case PATTERN_FOR_KEY_VALUE_ARG(key, value) if nextKV =>
            val (_, newValue) = redact(redactionPattern, Seq((key, value))).head
            nextKV = false
            genKeyValuePair(key, newValue)

          case cmd if cmd == CONF =>
            nextKV = true
            cmd

          case cmd =>
            cmd
        }
      case _ => commands
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

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def prettyPrint(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }

  def withLockRequired[T](lock: Lock)(block: => T): T = {
    try {
      lock.lock()
      block
    } finally {
      lock.unlock()
    }
  }

  /**
   * Try killing the process gracefully first, then forcibly if process does not exit in
   * graceful period.
   *
   * @param process the being killed process
   * @param gracefulPeriod the graceful killing period, in milliseconds
   * @return the exit code if process exit normally, None if the process finally was killed
   *         forcibly
   */
  def terminateProcess(process: java.lang.Process, gracefulPeriod: Long): Option[Int] = {
    process.destroy()
    if (process.waitFor(gracefulPeriod, TimeUnit.MILLISECONDS)) {
      Some(process.exitValue())
    } else {
      warn(s"Process does not exit after $gracefulPeriod ms, try to forcibly kill. " +
        "Staging files generated by the process may be retained!")
      process.destroyForcibly()
      None
    }
  }

}
