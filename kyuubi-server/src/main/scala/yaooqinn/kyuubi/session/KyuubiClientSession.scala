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

package yaooqinn.kyuubi.session

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{OperationHandle, OperationManager}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.spark.SparkSessionWithUGI

/**
 * An Execution Session with [[SparkSession]] instance inside, which shares [[SparkContext]]
 * with other sessions create by the same user.
 *
 * One user, one [[SparkContext]]
 * One user, multi [[IKyuubiSession]]s
 *
 * One [[IKyuubiSession]], one [[SparkSession]]
 * One [[SparkContext]], multi [[SparkSession]]s
 *
 */
private[kyuubi] class KyuubiClientSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager)
  extends AbstractKyuubiSession(
    protocol,
    username,
    password,
    conf,
    ipAddress,
    withImpersonation,
    sessionManager,
    operationManager) {

  private val sparkSessionWithUGI =
    new SparkSessionWithUGI(sessionUGI, conf, sessionManager.getCacheMgr)

  def sparkSession: SparkSession = this.sparkSessionWithUGI.sparkSession

  @throws[KyuubiSQLException]
  override def open(sessionConf: Map[String, String]): Unit = {
    sparkSessionWithUGI.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  override protected def getDbmsVersion: String = {
    this.sparkSessionWithUGI.sparkSession.version
  }

  override def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    acquire(true)
    try {
      operationManager.getResultSetSchema(opHandle)
    } finally {
      release(true)
    }
  }

  @throws[KyuubiSQLException]
  override def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    acquire(true)
    try {
      fetchType match {
        case FetchType.QUERY_OUTPUT =>
          operationManager.getOperationNextRowSet(opHandle, orientation, maxRows)
        case _ =>
          operationManager.getOperationLogRowSet(opHandle, orientation, maxRows)
      }
    } finally {
      release(true)
    }
  }
}
