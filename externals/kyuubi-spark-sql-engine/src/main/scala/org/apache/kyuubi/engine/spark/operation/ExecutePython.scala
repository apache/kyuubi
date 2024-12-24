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
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkFiles
import org.apache.spark.api.python.KyuubiPythonGatewayServer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_PYTHON_ENV_ARCHIVE, ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH, ENGINE_SPARK_PYTHON_HOME_ARCHIVE, ENGINE_SPARK_PYTHON_MAGIC_ENABLED}
import org.apache.kyuubi.config.KyuubiConf.EngineSparkOutputMode.{AUTO, EngineSparkOutputMode, NOTEBOOK}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_USER_KEY, KYUUBI_STATEMENT_ID_KEY}
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.util.JsonUtils
import org.apache.kyuubi.operation.{ArrayFetchIterator, OperationHandle, OperationState}
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.TempFileCleanupUtils
import org.apache.kyuubi.util.reflect.DynFields

class ExecutePython(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    worker: SessionPythonWorker,
    override protected val handle: OperationHandle) extends SparkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  override protected def supportProgress: Boolean = true

  override protected def resultSchema: StructType = {
    if (result == null) {
      new StructType().add("output", "string")
        .add("status", "string")
        .add("ename", "string")
        .add("evalue", "string")
        .add("traceback", "array<string>")
    } else {
      super.resultSchema
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def executePython(): Unit =
    try {
      withLocalProperties {
        setState(OperationState.RUNNING)
        info(diagnostics)
        addOperationListener()
        val response = worker.runCode(statement)
        val status = response.map(_.content.status).getOrElse("UNKNOWN_STATUS")
        if (PythonResponse.OK_STATUS.equalsIgnoreCase(status)) {
          val output = response.map(_.content.getOutput(outputMode)).getOrElse("")
          val ename = response.map(_.content.getEname()).getOrElse("")
          val evalue = response.map(_.content.getEvalue()).getOrElse("")
          val traceback = response.map(_.content.getTraceback()).getOrElse(Seq.empty)
          iter =
            new ArrayFetchIterator[Row](Array(Row(output, status, ename, evalue, traceback)))
          setState(OperationState.FINISHED)
        } else {
          throw KyuubiSQLException(s"Interpret error:\n" +
            s"${JsonUtils.toPrettyJson(Map("code" -> statement, "response" -> response.orNull))}")
        }
      }
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
          executePython()
        }
      }

      try {
        val sparkSQLSessionManager = session.sessionManager
        val backgroundHandle = sparkSQLSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke =
            KyuubiSQLException("Error submitting python in background", rejected)
          setOperationException(ke)
          shutdownTimeoutMonitor()
          throw ke
      }
    } else {
      executePython()
    }
  }

  override def setSparkLocalProperty: (String, String) => Unit =
    (key: String, value: String) => {
      val valueStr = if (value == null) "None" else s"'$value'"
      worker.runCode(s"spark.sparkContext.setLocalProperty('$key', $valueStr)", internal = true)
      ()
    }

  override protected def withLocalProperties[T](f: => T): T = {
    try {
      // to prevent the transferred set job group python code broken
      val jobDesc = s"Python statement: $statementId"
      // for python, the boolean value is capitalized
      val pythonForceCancel = if (forceCancel) "True" else "False"
      worker.runCode(
        "spark.sparkContext.setJobGroup" +
          s"('$statementId', '$jobDesc', $pythonForceCancel)",
        internal = true)
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
      // using cancelJobGroup for pyspark, see details in pyspark/context.py
      worker.runCode(s"spark.sparkContext.cancelJobGroup('$statementId')", internal = true)
      if (isSessionUserSignEnabled) {
        clearSessionUserSign()
      }
    }
  }

  override def cleanup(targetState: OperationState): Unit = {
    if (!isTerminalState(state)) {
      info(s"Staring to cancel python code: $statement")
      worker.interrupt()
    }
    super.cleanup(targetState)
  }
}

case class SessionPythonWorker(
    errorReader: Thread,
    pythonWorkerMonitor: Thread,
    workerProcess: Process) {
  private val stdin: PrintWriter = new PrintWriter(workerProcess.getOutputStream)
  private val stdout: BufferedReader =
    new BufferedReader(new InputStreamReader(workerProcess.getInputStream), 1)
  private val lock = new ReentrantLock()

  private def withLockRequired[T](block: => T): T = Utils.withLockRequired(lock)(block)

  /**
   * Run the python code and return the response. This method maybe invoked internally,
   * such as setJobGroup and cancelJobGroup, if the internal python code is not formatted correctly,
   * it might impact the correctness and even cause result out of sequence. To prevent that,
   * please make sure the internal python code simple and set internal flag, to be aware of the
   * internal python code failure.
   *
   * @param code the python code
   * @param internal whether is internal python code
   * @return the python response
   */
  def runCode(code: String, internal: Boolean = false): Option[PythonResponse] = withLockRequired {
    if (!workerProcess.isAlive) {
      throw KyuubiSQLException("Python worker process has been exited, please check the error log" +
        " and re-create the session to run python code.")
    }
    val input = JsonUtils.toJson(Map("code" -> code, "cmd" -> "run_code"))
    // scalastyle:off println
    stdin.println(input)
    // scalastyle:on
    stdin.flush()
    val pythonResponse = Option(stdout.readLine()).map(JsonUtils.fromJson[PythonResponse](_))
    // throw exception if internal python code fail
    if (internal && !pythonResponse.map(_.content.status).contains(PythonResponse.OK_STATUS)) {
      throw KyuubiSQLException(s"Internal python code $code failure: $pythonResponse")
    }
    pythonResponse
  }

  def close(): Unit = {
    val exitCmd = JsonUtils.toJson(Map("cmd" -> "exit_worker"))
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

  def interrupt(): Unit = {
    val pid = DynFields.builder()
      .hiddenImpl(workerProcess.getClass, "pid")
      .build[java.lang.Integer](workerProcess)
      .get()
    // sends a SIGINT (interrupt) signal, similar to Ctrl-C
    val builder = new ProcessBuilder(Seq("kill", "-2", pid.toString).asJava)
    val process = builder.start()
    val exitCode = process.waitFor()
    process.destroy()
    if (exitCode != 0) {
      error(s"Process `${builder.command().asScala.mkString(" ")}` exit with value: $exitCode")
    }
  }
}

object ExecutePython extends Logging {
  final val DEFAULT_SPARK_PYTHON_HOME_ARCHIVE_FRAGMENT = "__kyuubi_spark_python_home__"
  final val DEFAULT_SPARK_PYTHON_ENV_ARCHIVE_FRAGMENT = "__kyuubi_spark_python_env__"
  final val PY4J_REGEX = "py4j-[\\S]*.zip$".r
  final val PY4J_PATH = "PY4J_PATH"
  final val IS_PYTHON_APP_KEY = "spark.yarn.isPython"
  final val MAGIC_ENABLED = "MAGIC_ENABLED"

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
    val sessionId = session.handle.identifier.toString
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
    env.put("KYUUBI_SPARK_SESSION_UUID", sessionId)
    env.put("PYTHON_GATEWAY_CONNECTION_INFO", KyuubiPythonGatewayServer.CONNECTION_FILE_PATH)
    env.put(MAGIC_ENABLED, getSessionConf(ENGINE_SPARK_PYTHON_MAGIC_ENABLED, spark).toString)
    logger.info(
      s"""
         |launch python worker command: ${builder.command().asScala.mkString(" ")}
         |environment:
         |${builder.environment().asScala.map(kv => kv._1 + "=" + kv._2).mkString("\n")}
         |""".stripMargin)
    builder.redirectError(Redirect.PIPE)
    val process = builder.start()
    SessionPythonWorker(
      startStderrSteamReader(process, sessionId),
      startWatcher(process, sessionId),
      process)
  }

  def getSparkPythonExecFromArchive(spark: SparkSession, session: Session): Option[String] = {
    val pythonEnvArchive = getSessionConf(ENGINE_SPARK_PYTHON_ENV_ARCHIVE, spark)
    val pythonEnvExecPath = getSessionConf(ENGINE_SPARK_PYTHON_ENV_ARCHIVE_EXEC_PATH, spark)
    pythonEnvArchive.map {
      archive =>
        var uri = new URI(archive)
        if (uri.getFragment == null) {
          uri = buildURI(uri, DEFAULT_SPARK_PYTHON_ENV_ARCHIVE_FRAGMENT)
        }
        spark.sparkContext.addArchive(uri.toString)
        Paths.get(SparkFiles.get(uri.getFragment), pythonEnvExecPath)
    }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
  }

  def getSparkPythonHomeFromArchive(spark: SparkSession, session: Session): Option[String] = {
    val pythonHomeArchive = getSessionConf(ENGINE_SPARK_PYTHON_HOME_ARCHIVE, spark)
    pythonHomeArchive.map {
      archive =>
        var uri = new URI(archive)
        if (uri.getFragment == null) {
          uri = buildURI(uri, DEFAULT_SPARK_PYTHON_HOME_ARCHIVE_FRAGMENT)
        }
        spark.sparkContext.addArchive(uri.toString)
        Paths.get(SparkFiles.get(uri.getFragment))
    }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
  }

  // for test
  def defaultSparkHome: String = {
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

  private def startStderrSteamReader(process: Process, sessionId: String): Thread = {
    val stderrThread = new Thread(s"session[$sessionId] process stderr thread") {
      override def run(): Unit = {
        val lines = scala.io.Source.fromInputStream(process.getErrorStream).getLines()
        lines.filter(_.trim.nonEmpty).foreach(logger.error)
      }
    }
    stderrThread.setDaemon(true)
    stderrThread.start()
    stderrThread
  }

  def startWatcher(process: Process, sessionId: String): Thread = {
    val processWatcherThread = new Thread(s"session[$sessionId] process watcher thread") {
      override def run(): Unit = {
        try {
          val exitCode = process.waitFor()
          if (exitCode != 0) {
            logger.error(f"Process has died with $exitCode")
          }
        } catch {
          case _: InterruptedException => logger.warn("Process has been interrupted")
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
    TempFileCleanupUtils.deleteOnExit(file)

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
}

case class PythonResponse(
    msg_type: String,
    content: PythonResponseContent)

object PythonResponse {
  final val OK_STATUS = "ok"
}

case class PythonResponseContent(
    data: Map[String, Object],
    ename: String,
    evalue: String,
    traceback: Seq[String],
    status: String) {
  def getOutput(outputMode: EngineSparkOutputMode): String = {
    if (data == null) return ""

    outputMode match {
      case AUTO =>
        // If data does not contains field other than `test/plain`, keep backward compatibility,
        // otherwise, return all the data.
        if (data.filterNot(_._1 == "text/plain").isEmpty) {
          data.get("text/plain").map {
            case str: String => str
            case obj => JsonUtils.toJson(obj)
          }.getOrElse("")
        } else {
          JsonUtils.toJson(data)
        }
      case NOTEBOOK => JsonUtils.toJson(data)
    }
  }
  def getEname(): String = {
    Option(ename).getOrElse("")
  }
  def getEvalue(): String = {
    Option(evalue).getOrElse("")
  }
  def getTraceback(): Seq[String] = {
    Option(traceback).getOrElse(Seq.empty)
  }
}
