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

import java.io.File
import java.util.concurrent.RejectedExecutionException

import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import scala.tools.nsc.interpreter.Results.{Error, Incomplete, Success}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil._
import org.apache.kyuubi.engine.spark.repl.KyuubiSparkILoop
import org.apache.kyuubi.engine.spark.util.JsonUtils
import org.apache.kyuubi.operation.{ArrayFetchIterator, OperationHandle, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

/**
 * Support executing Scala Script with or without common Spark APIs, only support running in sync
 * mode, as an operation may [[Incomplete]] and wait for others to make [[Success]].
 *
 * [[KyuubiSparkILoop.result]] is exposed as a [[org.apache.spark.sql.DataFrame]] holder to users
 * in repl to transfer result they wanted to client side.
 *
 * @param session parent session
 * @param repl Scala Interpreter
 * @param statement a scala code snippet
 */
class ExecuteScala(
    session: Session,
    repl: KyuubiSparkILoop,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    override protected val handle: OperationHandle)
  extends SparkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)
  override protected def supportProgress: Boolean = true

  override protected def resultSchema: StructType = {
    if (result == null) {
      new StructType().add("output", "string")
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

  private def executeScala(): Unit =
    try {
      withLocalProperties {
        setState(OperationState.RUNNING)
        info(diagnostics)
        Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
        addOperationListener()
        val legacyOutput = repl.getOutput
        if (legacyOutput.nonEmpty) {
          warn(s"Clearing legacy output from last interpreting:\n $legacyOutput")
        }
        val replUrls = repl.classLoader.getParent.asInstanceOf[URLClassLoader].getURLs
        spark.sharedState.jarClassLoader.getURLs.filterNot(replUrls.contains).foreach { jar =>
          try {
            if ("file".equals(jar.toURI.getScheme)) {
              repl.addUrlsToClassPath(jar)
            } else {
              spark.sparkContext.addFile(jar.toString)
              val localJarFile = new File(SparkFiles.get(new Path(jar.toURI.getPath).getName))
              val localJarUrl = localJarFile.toURI.toURL
              if (!replUrls.contains(localJarUrl)) {
                repl.addUrlsToClassPath(localJarUrl)
              }
            }
          } catch {
            case e: Throwable => error(s"Error adding $jar to repl class path", e)
          }
        }

        repl.interpretWithRedirectOutError(statement) match {
          case Success =>
            iter = {
              result = repl.getResult(statementId)
              if (result != null) {
                new ArrayFetchIterator[Row](result.collect())
              } else {
                val output = repl.getOutput
                debug("scala repl output:\n" + output)
                new ArrayFetchIterator[Row](Array(Row(output)))
              }
            }
          case Error =>
            throw KyuubiSQLException(s"Interpret error:\n" +
              s"${JsonUtils.toPrettyJson(Map("code" -> statement, "response" -> repl.getOutput))}")
          case Incomplete =>
            throw KyuubiSQLException(s"Incomplete code:\n$statement")
        }
        setState(OperationState.FINISHED)
      }
    } catch {
      onError(cancel = true)
    } finally {
      repl.clearResult(statementId)
      shutdownTimeoutMonitor()
    }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
          executeScala()
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
            KyuubiSQLException("Error submitting scala in background", rejected)
          setOperationException(ke)
          shutdownTimeoutMonitor
          throw ke
      }
    } else {
      executeScala()
    }
  }
}
