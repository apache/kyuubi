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

package org.apache.kyuubi.engine.spark.operation

import java.io.{BufferedReader, File, FilenameFilter, FileOutputStream, InputStreamReader, PrintWriter}
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import javax.ws.rs.core.UriBuilder

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkFiles
import org.apache.spark.api.python.KyuubiPythonGatewayServer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_PYTHON_ENV_ARCHIVE, ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH, ENGINE_SPARK_PYTHON_HOME_ARCHIVE}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_USER_KEY, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.operation.ArrayFetchIterator
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecutePython(
    session: Session,
    override val statement: String,
    worker: SessionPythonWorker) extends SparkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("output", "string")
        .add("status", "string")
        .add("ename", "string")
        .add("evalue", "string")
        .add("traceback", "array<string>")
    } else {
      result.schema
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    super.beforeRun()
  }

  override protected def runInternal(): Unit = withLocalProperties {
    try {
      info(diagnostics)
      val response = worker.runCode(statement)
      val output = response.map(_.content.getOutput()).getOrElse("")
      val status = response.map(_.content.status).getOrElse("UNKNOWN_STATUS")
      val ename = response.map(_.content.getEname()).getOrElse("")
      val evalue = response.map(_.content.getEvalue()).getOrElse("")
      val traceback = response.map(_.content.getTraceback()).getOrElse(Array.empty)
      iter =
        new ArrayFetchIterator[Row](Array(Row(output, status, ename, evalue, Row(traceback: _*))))
    } catch {
      onError(cancel = true)
    }
  }

  override def setSparkLocalProperty: (String, String) => Unit =
    (key: String, value: String) => {
      val valueStr = if (value == null) "None" else s"'$value'"
      worker.runCode(s"spark.sparkContext.setLocalProperty('$key', $valueStr)")
      ()
    }

  override protected def withLocalProperties[T](f: => T): T = {
    try {
      worker.runCode("spark.sparkContext.setJobGroup" +
        s"($statementId, $redactedStatement, $forceCancel)")
      setSparkLocalProperty(KYUUBI_SESSION_USER_KEY, session.user)
      setSparkLocalProperty(KYUUBI_STATEMENT_ID_KEY, statementId)
      schedulerPool match {
        case Some(pool) =>
          setSparkLocalProperty(SPARK_SCHEDULER_POOL_KEY, pool)
        case None =>
      }
      if (isSessionUserSignEnabled) {
        setSessionUserSign()
      }

      f
    } finally {
      setSparkLocalProperty(KYUUBI_SESSION_USER_KEY, "")
      setSparkLocalProperty(KYUUBI_STATEMENT_ID_KEY, "")
      setSparkLocalProperty(SPARK_SCHEDULER_POOL_KEY, "")
      worker.runCode("spark.sparkContext.clearJobGroup()")
      if (isSessionUserSignEnabled) {
        clearSessionUserSign()
      }
    }
  }
}

case class SessionPythonWorker(
    errorReader: Thread,
    pythonWorkerMonitor: Thread,
    workerProcess: Process) {
  private val stdin: PrintWriter = new PrintWriter(workerProcess.getOutputStream)
  private val stdout: BufferedReader =
    new BufferedReader(new InputStreamReader(workerProcess.getInputStream), 1)

  def runCode(code: String): Option[PythonResponse] = {
    val input = ExecutePython.toJson(Map("code" -> code, "cmd" -> "run_code"))
    // scalastyle:off println
    stdin.println(input)
    // scalastyle:on
    stdin.flush()
    Option(stdout.readLine())
      .map(ExecutePython.fromJson[PythonResponse](_))
  }

  def close(): Unit = {
    val exitCmd = ExecutePython.toJson(Map("cmd" -> "exit_worker"))
    // scalastyle:off println
    stdin.println(exitCmd)
    // scalastyle:on
    stdin.flush()
    stdin.close()
    stdout.close()
    errorReader.interrupt()
    pythonWorkerMonitor.interrupt()
    workerProcess.destroy()
  }
}

object ExecutePython extends Logging {
  final val DEFAULT_SPARK_PYTHON_HOME_ARCHIVE_FRAGMENT = "__kyuubi_spark_python_home__"
  final val DEFAULT_SPARK_PYTHON_ENV_ARCHIVE_FRAGMENT = "__kyuubi_spark_python_env__"
  final val PY4J_REGEX = "py4j-[\\S]*.zip$".r
  final val PY4J_PATH = "PY4J_PATH"
  final val IS_PYTHON_APP_KEY = "spark.yarn.isPython"

  private val isPythonGatewayStart = new AtomicBoolean(false)
  private val kyuubiPythonPath = Utils.createTempDir()
  def init(): Unit = {
    if (!isPythonGatewayStart.get()) {
      synchronized {
        if (!isPythonGatewayStart.get()) {
          KyuubiPythonGatewayServer.start()
          writeTempPyFile(kyuubiPythonPath, "execute_python.py")
          writeTempPyFile(kyuubiPythonPath, "kyuubi_util.py")
          isPythonGatewayStart.set(true)
        }
      }
    }
  }

  def createSessionPythonWorker(spark: SparkSession, session: Session): SessionPythonWorker = {
    val pythonExec = StringUtils.firstNonBlank(
      spark.conf.getOption("spark.pyspark.driver.python").orNull,
      spark.conf.getOption("spark.pyspark.python").orNull,
      System.getenv("PYSPARK_DRIVER_PYTHON"),
      System.getenv("PYSPARK_PYTHON"),
      getSparkPythonExecFromArchive(spark, session).getOrElse("python3"))

    val builder = new ProcessBuilder(Seq(
      pythonExec,
      s"${ExecutePython.kyuubiPythonPath}/execute_python.py").asJava)
    val env = builder.environment()
    val pythonPath = sys.env.getOrElse("PYTHONPATH", "")
      .split(File.pathSeparator)
      .++(ExecutePython.kyuubiPythonPath.toString)
    env.put("PYTHONPATH", pythonPath.mkString(File.pathSeparator))
    // try to find py4j lib from `PYTHONPATH` and set env `PY4J_PATH` into process if found
    pythonPath.mkString(File.pathSeparator)
      .split(File.pathSeparator)
      .find(PY4J_REGEX.findFirstMatchIn(_).nonEmpty)
      .foreach(env.put(PY4J_PATH, _))
    if (!spark.sparkContext.getConf.getBoolean(IS_PYTHON_APP_KEY, false)) {
      env.put(
        "SPARK_HOME",
        sys.env.getOrElse(
          "SPARK_HOME",
          getSparkPythonHomeFromArchive(spark, session).getOrElse(defaultSparkHome)))
    }
    env.put("PYTHON_GATEWAY_CONNECTION_INFO", KyuubiPythonGatewayServer.CONNECTION_FILE_PATH)
    logger.info(
      s"""
         |launch python worker command: ${builder.command().asScala.mkString(" ")}
         |environment:
         |${builder.environment().asScala.map(kv => kv._1 + "=" + kv._2).mkString("\n")}
         |""".stripMargin)
    builder.redirectError(Redirect.PIPE)
    val process = builder.start()
    SessionPythonWorker(startStderrSteamReader(process), startWatcher(process), process)
  }

  def getSparkPythonExecFromArchive(spark: SparkSession, session: Session): Option[String] = {
    val pythonEnvArchive = spark.conf.getOption(ENGINE_SPARK_PYTHON_ENV_ARCHIVE.key)
      .orElse(session.sessionManager.getConf.get(ENGINE_SPARK_PYTHON_ENV_ARCHIVE))
    val pythonEnvExecPath = spark.conf.getOption(ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH.key)
      .getOrElse(session.sessionManager.getConf.get(ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH))
    pythonEnvArchive.map {
      archive =>
        var uri = new URI(archive)
        if (uri.getFragment == null) {
          uri = UriBuilder.fromUri(uri).fragment(DEFAULT_SPARK_PYTHON_ENV_ARCHIVE_FRAGMENT).build()
        }
        spark.sparkContext.addArchive(uri.toString)
        Paths.get(SparkFiles.get(uri.getFragment), pythonEnvExecPath)
    }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
  }

  def getSparkPythonHomeFromArchive(spark: SparkSession, session: Session): Option[String] = {
    val pythonHomeArchive = spark.conf.getOption(ENGINE_SPARK_PYTHON_HOME_ARCHIVE.key)
      .orElse(session.sessionManager.getConf.get(ENGINE_SPARK_PYTHON_HOME_ARCHIVE))
    pythonHomeArchive.map {
      archive =>
        var uri = new URI(archive)
        if (uri.getFragment == null) {
          uri = UriBuilder.fromUri(uri).fragment(DEFAULT_SPARK_PYTHON_HOME_ARCHIVE_FRAGMENT).build()
        }
        spark.sparkContext.addArchive(uri.toString)
        Paths.get(SparkFiles.get(uri.getFragment))
    }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
  }

  // for test
  def defaultSparkHome(): String = {
    val homeDirFilter: FilenameFilter = (dir: File, name: String) =>
      dir.isDirectory && name.contains("spark-") && !name.contains("-engine")
    // get from kyuubi-server/../externals/kyuubi-download/target
    new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
      .split("kyuubi-spark-sql-engine").flatMap { cwd =>
        val candidates = Paths.get(cwd, "kyuubi-download", "target")
          .toFile.listFiles(homeDirFilter)
        if (candidates == null) None else candidates.map(_.toPath).headOption
      }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
      .getOrElse {
        throw new IllegalStateException("SPARK_HOME not found!")
      }
  }

  private def startStderrSteamReader(process: Process): Thread = {
    val stderrThread = new Thread("process stderr thread") {
      override def run(): Unit = {
        val lines = scala.io.Source.fromInputStream(process.getErrorStream).getLines()
        lines.foreach(logger.error)
      }
    }
    stderrThread.setDaemon(true)
    stderrThread.start()
    stderrThread
  }

  def startWatcher(process: Process): Thread = {
    val processWatcherThread = new Thread("process watcher thread") {
      override def run(): Unit = {
        val exitCode = process.waitFor()
        if (exitCode != 0) {
          logger.error(f"Process has died with $exitCode")
        }
      }
    }
    processWatcherThread.setDaemon(true)
    processWatcherThread.start()
    processWatcherThread
  }

  private def writeTempPyFile(pythonPath: Path, pyfile: String): File = {
    val source = getClass.getClassLoader.getResourceAsStream(s"python/$pyfile")

    val file = new File(pythonPath.toFile, pyfile)
    file.deleteOnExit()

    val sink = new FileOutputStream(file)
    val buf = new Array[Byte](1024)
    var n = source.read(buf)

    while (n > 0) {
      sink.write(buf, 0, n)
      n = source.read(buf)
    }
    source.close()
    sink.close()
    file
  }

  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  def toJson[T](obj: T): String = {
    mapper.writeValueAsString(obj)
  }
  def fromJson[T](json: String, clz: Class[T]): T = {
    mapper.readValue(json, clz)
  }

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue(json, m.runtimeClass).asInstanceOf[T]
  }

}

case class PythonResponse(
    msg_type: String,
    content: PythonResponseContent)

case class PythonResponseContent(
    data: Map[String, String],
    ename: String,
    evalue: String,
    traceback: Array[String],
    status: String) {
  def getOutput(): String = {
    Option(data)
      .map(_.getOrElse("text/plain", ""))
      .getOrElse("")
  }
  def getEname(): String = {
    Option(ename).getOrElse("")
  }
  def getEvalue(): String = {
    Option(evalue).getOrElse("")
  }
  def getTraceback(): Array[String] = {
    Option(traceback).getOrElse(Array.empty)
  }
}
