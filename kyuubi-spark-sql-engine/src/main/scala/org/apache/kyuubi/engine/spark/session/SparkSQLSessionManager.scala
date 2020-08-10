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

package org.apache.kyuubi.engine.spark.session

import scala.util.control.NonFatal

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.operation.SparkSQLOperationManager
import org.apache.kyuubi.session.{SessionHandle, SessionManager}


/**
 * A [[SessionManager]] constructed with [[SparkSession]] which give it the ability to talk with
 * Spark and let Spark do all the rest heavy work :)
 *
 *  @param name Service Name
 * @param spark A [[SparkSession]] instance that this [[SessionManager]] holds to create individual
 *              [[SparkSession]] for [[org.apache.kyuubi.session.Session]]s.
 */
class SparkSQLSessionManager private (name: String, spark: SparkSession)
  extends SessionManager(name) {

  def this(spark: SparkSession) = this(classOf[SparkSQLSessionManager].getSimpleName, spark)

  val operationManager = new SparkSQLOperationManager()

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    val sessionImpl = new SparkSessionImpl(protocol, user, password, ipAddress, conf, this)
    val handle = sessionImpl.handle
    try {
      val sparkSession = spark.newSession()
      conf.foreach { case (key, value) => spark.conf.set(key, value)}
      operationManager.setSparkSession(handle, sparkSession)
      info(s"$user's session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")

      setSession(handle, sessionImpl)
      handle
    } catch {
      case NonFatal(e) =>
        try {
          sessionImpl.close()
        } catch {
          case t: Throwable => warn(s"Error closing session $handle for $user", t)

        }
        throw KyuubiSQLException(s"Error opening session $handle for $user", e)
    }
  }
}
