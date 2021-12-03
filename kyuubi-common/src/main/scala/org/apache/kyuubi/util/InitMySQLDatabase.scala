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

package org.apache.kyuubi.util

import java.sql.{PreparedStatement, Statement}

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_EVENT_STORE_JDBC_URL

/**
 * This object is used for init datasource, get connection and generate base_sql
 */
object InitMySQLDatabase {

  private val mysqlName = "com.mysql.cj.jdbc.Driver"
  private var druidDataSource: DruidDataSource = null

  def initialize(conf: KyuubiConf): Unit = {
    if (null == druidDataSource) {
      val jdbcUrl = conf.get(ENGINE_EVENT_STORE_JDBC_URL).get
      druidDataSource = new DruidDataSource()
      druidDataSource.setDriverClassName(mysqlName)
      druidDataSource.setUrl(jdbcUrl)
    }
  }

  def getConnection(): DruidPooledConnection = {
    var druidConnection: DruidPooledConnection = null
    try {
      druidConnection = druidDataSource.getConnection
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException(s"Create datasource connection failed.")
    }
    return druidConnection
  }

  def close(
      connection: DruidPooledConnection,
      preparedStatement: PreparedStatement,
      statement: Statement): Unit = {
    if (null != preparedStatement) {
      preparedStatement.close()
    }
    if (null != connection) {
      connection.close()
    }
    if (null != statement) {
      statement.close()
    }
  }

  def insertSQL(tableName: String, insertFields: String): StringBuilder = {
    val stringBuilder: StringBuilder = new StringBuilder("insert into ")
      .append(tableName)
      .append(" ( ")
      .append(insertFields)
      .append(" ) values ( ")
      .append(formQuestionMark(insertFields))
      .append(");")
    return stringBuilder
  }

  def insertOrUpdateSQL(
      tableName: String,
      insertFields: String,
      updateFields: Array[String]): StringBuilder = {
    val stringBuilder: StringBuilder = new StringBuilder("insert into ")
      .append(tableName)
      .append(" ( ")
      .append(insertFields)
      .append(" ) ")
      .append(" values ")
      .append(" ( ")
      .append(formQuestionMark(insertFields))
      .append(" ) ON DUPLICATE KEY UPDATE ")

    for (i <- 0 until updateFields.length) {
      stringBuilder.append(updateFields.apply(i))
        .append("=?")
      if (i < updateFields.length - 1) {
        stringBuilder.append(",")
      }
    }
    return stringBuilder
  }

  def updateSessionSQL(): StringBuilder = {
    val stringBuilder: StringBuilder = new StringBuilder("update ")
      .append("session_event_summary set complete_time=?,")
      .append("total_operations=if(total_operations <?,?,total_operations) where session_id=?")
    return stringBuilder
  }

  private def formQuestionMark(fields: String): String = {
    val arr = fields.split(",")
    val stringBuilder: StringBuilder = new StringBuilder()
    if (arr.length >= 1 && !"".equals(arr(0).trim)) {
      stringBuilder.append("?")
    }
    for (_ <- 1 until arr.length) {
      stringBuilder.append(", ?")
    }
    return stringBuilder.toString()
  }
}
