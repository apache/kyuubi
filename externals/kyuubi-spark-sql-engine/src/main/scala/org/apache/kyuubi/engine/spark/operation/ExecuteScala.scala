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

import scala.tools.nsc.interpreter.Results.{Error, Incomplete, Success}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.ArrayFetchIterator
import org.apache.kyuubi.engine.spark.repl.KyuubiSparkILoop
import org.apache.kyuubi.operation.OperationType
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
  extends SparkOperation(OperationType.EXECUTE_STATEMENT, session) {

  override protected def resultSchema: StructType = {
    if (result == null || result.schema.isEmpty) {
      new StructType().add("output", "string")
    } else {
      result.schema
    }
  }

  override protected def runInternal(): Unit = {
    try {
      spark.sparkContext.setJobGroup(statementId, statement)
      Thread.currentThread().setContextClassLoader(spark.sharedState.jarClassLoader)
      repl.interpret(statement) match {
        case Success =>
          iter = {
            result = repl.getResult
            if (result != null) {
              new ArrayFetchIterator[Row](result.collect())
            } else {
              // TODO (#1498): Maybe we shall pass the output through operation log
              // but some clients may not support operation log
              new ArrayFetchIterator[Row](Array(Row(repl.getOutput)))
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
      spark.sparkContext.clearJobGroup()
    }
  }
}
