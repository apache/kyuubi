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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType, GetInfoValue}
import yaooqinn.kyuubi.operation.OperationHandle
import yaooqinn.kyuubi.schema.RowSet

/**
 * Interface class of kyuubi session.
 */
trait IKyuubiSession {

  /**
   * Get this IKyuubiSession's user group information, which used to execute some
   * behaviors with relative privilege.
   */
  def ugi: UserGroupInformation

  /**
   * Open this IKyuubiSession.
   * @param sessionConf
   * @throws KyuubiSQLException
   * @return
   */
  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit

  /**
   * Get session info.
   * @param getInfoType such as SERVER_NAME, DBMS_NAME and DBMS_VERSION.
   * @return
   */
  def getInfo(getInfoType: GetInfoType): GetInfoValue

  /**
   * Execute sql statement.
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatement(statement: String): OperationHandle

  /**
   * Execute sql statement asynchronously.
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatementAsync(statement: String): OperationHandle

  /**
   * Close this IKyuubiSession.
   * @throws
   */
  @throws[KyuubiSQLException]
  def close(): Unit

  /**
   * Cancel relative IKyuubiOperation.
   * @param opHandle
   * @return
   */
  def cancelOperation(opHandle: OperationHandle): Unit

  /**
   * Close relative IKyuubiOperation.
   * @param opHandle
   * @return
   */
  def closeOperation(opHandle: OperationHandle): Unit

  /**
   * Get schema of result set.
   * @param opHandle
   * @return
   */
  def getResultSetMetadata(opHandle: OperationHandle): StructType

  /**
   * Fetch results of relative IKyuubiOperation.
   * @param opHandle
   * @param orientation fetch orientation, FETCH_FIRST or FETCH_NEXT.
   * @param maxRows limit of fetch size.
   * @param fetchType QUERY_OUTPUT or LOG.
   * @throws KyuubiSQLException
   * @return
   */
  @throws[KyuubiSQLException]
  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet

  /**
   * Get this IKyuubiSession's delegation token.
   * @param authFactory
   * @param owner
   * @param renewer
   * @throws KyuubiSQLException
   * @return
   */
  @throws[KyuubiSQLException]
  def getDelegationToken(authFactory: KyuubiAuthFactory, owner: String, renewer: String): String

  /**
   * Cancel this IKyuubiSession's delegation token.
   * @param authFactory
   * @param tokenStr
   * @throws KyuubiSQLException
   */
  @throws[KyuubiSQLException]
  def cancelDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit

  /**
   * Renew this IKyuubiSession's delegation token.
   * @param authFactory
   * @param tokenStr
   * @throws KyuubiSQLException
   */
  @throws[KyuubiSQLException]
  def renewDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit

  /**
   * Close the this IKyuubiSession's expired operations.
   */
  def closeExpiredOperations: Unit

  /**
   * Get the idle duration from last access, return 0 if running operation exists.
   * @return
   */
  def getNoOperationTime: Long

  /**
   * Get protocol version of this IKyuubiSession.
   */
  def getProtocolVersion: TProtocolVersion

  /**
   * Get relative session handle.
   */
  def getSessionHandle: SessionHandle

  /**
   * Get this IKyuubiSession's user name.
   */
  def getUserName: String

  /**
   * Get pass word of relative user.
   */
  def getPassword: String

  /**
   * Get relative saslServer ipAddress.
   * @return
   */
  def getIpAddress: String

  /**
   * Get last access time.
   */
  def getLastAccessTime: Long

  /**
   * Get relative session manager.
   */
  def getSessionMgr: SessionManager
}
