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

package org.apache.kyuubi.plugin

import scala.util.control.NonFatal

import org.apache.kyuubi.KyuubiSQLException

/** A concrete, immutable [[StatementInterceptContext]] built per interceptor invocation. */
private[kyuubi] class StatementInterceptContextImpl(
    override val sessionId: String,
    override val statementId: String,
    override val user: String,
    override val realUser: String,
    override val ipAddress: String,
    override val statement: String,
    override val confOverlay: java.util.Map[String, String],
    override val runAsync: Boolean,
    override val queryTimeout: Long,
    override val engineType: String) extends StatementInterceptContext

private[kyuubi] object StatementInterception {

  /**
   * Initialize interceptors in order. If an interceptor's initialize throws, every interceptor
   * whose initialization was attempted is closed in reverse order before the error is rethrown, so
   * that a failed server startup does not leak resources (threads, connections, external clients)
   * allocated before the failure. Errors raised while closing are attached as suppressed
   * exceptions.
   */
  def initialize(
      interceptors: Seq[StatementInterceptor],
      conf: java.util.Map[String, String]): Unit = {
    val initialized = new scala.collection.mutable.ArrayBuffer[StatementInterceptor]()
    try {
      interceptors.foreach { interceptor =>
        initialized += interceptor
        interceptor.initialize(conf)
      }
    } catch {
      case initError: Throwable =>
        initialized.reverseIterator.foreach { interceptor =>
          try {
            interceptor.close()
          } catch {
            case NonFatal(closeError) => initError.addSuppressed(closeError)
          }
        }
        throw initError
    }
  }

  /**
   * Run the interceptor chain over a statement. Each interceptor sees the statement produced by the
   * previous one (rewrite chaining); a reject, an exception, or a null result fails the statement.
   *
   * @param interceptors the interceptors in execution order
   * @param initialStatement the original statement
   * @param contextFor builds a context for the given (possibly rewritten) statement
   * @return the final statement to route to the engine
   */
  def run(
      interceptors: Seq[StatementInterceptor],
      initialStatement: String,
      contextFor: String => StatementInterceptContext): String = {
    if (interceptors.isEmpty) return initialStatement
    var current = initialStatement
    interceptors.foreach { interceptor =>
      val interceptorName = interceptor.getClass.getName
      val result =
        try {
          interceptor.beforeExecuteStatement(contextFor(current))
        } catch {
          case e: KyuubiSQLException => throw e
          case NonFatal(e) =>
            throw KyuubiSQLException(s"Statement interceptor $interceptorName failed", e)
        }
      if (result == null) {
        throw KyuubiSQLException(s"Statement interceptor $interceptorName returned null")
      }
      result.action() match {
        case StatementInterceptResult.Action.PROCEED => // keep the current statement
        case StatementInterceptResult.Action.REWRITE => current = result.statement()
        case StatementInterceptResult.Action.REJECT =>
          // SQLState 42501 = "Insufficient Privilege". Use the specific access-rule subclass so
          // clients can distinguish a policy rejection from the generic syntax-error class 42000.
          throw KyuubiSQLException(result.message(), sqlState = "42501")
      }
    }
    current
  }
}
