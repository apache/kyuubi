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

package org.apache.kyuubi.server.metadata.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.server.metadata.UserCustomStore

class JDBCUserCustomStore extends UserCustomStore with Logging {

  override def close(): Unit = {}

  override def getUserLimitList(): Seq[Tuple2[String, Int]] = {
    val query =
      s"""SELECT
         |u.NAME AS userName,
         |luser.num AS userLimit
         |FROM
         |t_sys_user u
         |JOIN t_limit_user luser ON u.id = luser.user_id""".stripMargin
    withConnection() { connection =>
      withResultSet(connection, query, ListBuffer[Any](): _*) { rs =>
        buildUserConfig(rs)
      }
    }
  }

  private def buildUserConfig(resultSet: ResultSet): Seq[Tuple2[String, Int]] = {
    val userMetaList = ListBuffer[Tuple2[String, Int]]()
    try {
      while (resultSet.next()) {
        val userName = resultSet.getString("userName")
        val userLimit = resultSet.getInt("userLimit")
        val userMeta = Tuple2(userName, userLimit)
        userMetaList += userMeta
      }
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
    userMetaList
  }

  override def getUserGroupList(): Seq[Tuple2[String, String]] = {
    val query =
      s"""SELECT
         |u.NAME AS userName,
         |g.NAME AS groupName
         |FROM
         |t_sys_user u
         |JOIN t_sys_group g ON u.group_id = g.id""".stripMargin
    withConnection() { connection =>
      withResultSet(connection, query, ListBuffer[Any](): _*) { rs =>
        buildGroupConfig(rs)
      }
    }
  }

  private def buildGroupConfig(resultSet: ResultSet): Seq[Tuple2[String, String]] = {
    val groupMetaList = ListBuffer[Tuple2[String, String]]()
    try {
      while (resultSet.next()) {
        val userName = resultSet.getString("userName")
        val groupName = resultSet.getString("groupName")
        val groupMeta = Tuple2(userName, groupName)
        groupMetaList += groupMeta
      }
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
    groupMetaList
  }

  private def withResultSet[T](
      conn: Connection,
      sql: String,
      params: Any*)(f: ResultSet => T): T = {
    debug(s"executing sql $sql with result set")
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      resultSet = statement.executeQuery()
      f(resultSet)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (resultSet != null) {
        Utils.tryLogNonFatalError(resultSet.close())
      }
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
  }

  private def setStatementParams(statement: PreparedStatement, params: Any*): Unit = {
    params.zipWithIndex.foreach { case (param, index) =>
      param match {
        case null => statement.setObject(index + 1, null)
        case s: String => statement.setString(index + 1, s)
        case i: Int => statement.setInt(index + 1, i)
        case l: Long => statement.setLong(index + 1, l)
        case d: Double => statement.setDouble(index + 1, d)
        case f: Float => statement.setFloat(index + 1, f)
        case b: Boolean => statement.setBoolean(index + 1, b)
        case _ => throw new KyuubiException(s"Unsupported param type ${param.getClass.getName}")
      }
    }
  }

  private def withConnection[T](autoCommit: Boolean = true)(f: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = JDBCMetadataDatasource.hikariDataSource.get.getConnection
      connection.setAutoCommit(autoCommit)
      f(connection)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (connection != null) {
        Utils.tryLogNonFatalError(connection.close())
      }
    }
  }

}

object JDBCUserCustomStore extends Logging {
  def apply(): JDBCUserCustomStore = {
    new JDBCUserCustomStore()
  }
}
