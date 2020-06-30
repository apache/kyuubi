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

import java.sql.SQLException

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TStatus, TStatusCode}

case class KyuubiSQLException(msg: String, cause: Throwable) extends SQLException(msg, cause) {
  /**
   * Converts current object to a [[TStatus]] object
   *
   * @return a { @link TStatus} object
   */
  def toTStatus: TStatus = {
    val tStatus = new TStatus(TStatusCode.ERROR_STATUS)
    tStatus.setSqlState(getSQLState)
    tStatus.setErrorCode(getErrorCode)
    tStatus.setErrorMessage(getMessage)
    tStatus.setInfoMessages(KyuubiSQLException.toString(this).asJava)
    tStatus
  }
}

object KyuubiSQLException {

  def apply(cause: Throwable): KyuubiSQLException = {
    new KyuubiSQLException(cause.getMessage, cause)
  }

  def apply(msg: String): KyuubiSQLException = new KyuubiSQLException(msg, null)

  def toTStatus(e: Exception): TStatus = e match {
    case k: KyuubiSQLException => k.toTStatus
    case _ =>
      val tStatus = new TStatus(TStatusCode.ERROR_STATUS)
      tStatus.setErrorMessage(e.getMessage)
      tStatus.setInfoMessages(toString(e).asJava)
      tStatus
  }

  def toString(cause: Throwable): List[String] = {
    toString(cause, null)
  }

  def toString(cause: Throwable, parent: Array[StackTraceElement]): List[String] = {
    val trace = cause.getStackTrace
    var m = trace.length - 1

    if (parent != null) {
      var n = parent.length - 1
      while (m >= 0 && n >=0 && trace(m).equals(parent(n))) {
        m = m - 1
        n = n - 1
      }
    }

    enroll(cause, trace, m) ++
      Option(cause.getCause).map(toString(_, trace)).getOrElse(Nil)
  }

  private def enroll(
      ex: Throwable,
      trace: Array[StackTraceElement],
      max: Int): List[String] = {
    val builder = new StringBuilder
    builder.append('*').append(ex.getClass.getName).append(':')
    builder.append(ex.getMessage).append(':')
    builder.append(trace.length).append(':').append(max)
    List(builder.toString) ++ (0 to max).map { i =>
      builder.setLength(0)
      builder.append(trace(i).getClassName).append(":")
      builder.append(trace(i).getMethodName).append(":")
      builder.append(Option(trace(i).getFileName).getOrElse("")).append(':')
      builder.append(trace(i).getLineNumber)
      builder.toString
    }.toList
  }
}
