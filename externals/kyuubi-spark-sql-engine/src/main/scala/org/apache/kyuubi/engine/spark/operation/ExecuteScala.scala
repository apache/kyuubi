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

import java.net.URL

import scala.tools.nsc.interpreter.Results.{Error, Incomplete, Success}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.repl.KyuubiSparkILoop
import org.apache.kyuubi.operation.ArrayFetchIterator
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
    override val statement: String)
  extends SparkOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("output", "string")
    } else {
      result.schema
    }
  }

  override protected def runInternal(): Unit = withLocalProperties {
    try {
      OperationLog.setCurrentOperationLog(operationLog)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      val legacyOutput = repl.getOutput
      if (legacyOutput.nonEmpty) {
        warn(s"Clearing legacy output from last interpreting:\n $legacyOutput")
      }
      spark.sharedState.jarClassLoader.getURLs.foreach { jar =>
        try {
          if ("file".equals(jar.toURI.getScheme)) {
            repl.addUrlsToClassPath(jar)
          } else {
            val fileName = new Path(jar.toURI.getPath).getName
            val localJarUrl = new URL(SparkFiles.get(fileName))
            repl.addUrlsToClassPath(localJarUrl)
          }
        } catch {
          case e: Throwable => error(s"Error adding $jar to class loader", e)
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
              info("scala repl output:\n" + output)
              new ArrayFetchIterator[Row](Array(Row(output)))
            }
          }
        case Error =>
          throw KyuubiSQLException(s"Interpret error:\n$statement\n ${repl.getOutput}")
        case Incomplete =>
          throw KyuubiSQLException(s"Incomplete code:\n$statement")
      }
    } catch {
      onError(cancel = true)
    } finally {
      repl.clearResult(statementId)
    }
  }
}
