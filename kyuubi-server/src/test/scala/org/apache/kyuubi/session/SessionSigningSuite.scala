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

package org.apache.kyuubi.session

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_SIGN_PUBLICKEY, KYUUBI_SESSION_USER_SIGN}
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.SignUtils

class SessionSigningSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected lazy val user: String = "user"

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(SESSION_USER_SIGN_ENABLED.key, "true")
  }

  test("KYUUBI #3839 session user sign - not allow override `kyuubi.session.user.sign.enabled`") {
    val checkSessionUser: Unit = {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET $KYUUBI_SESSION_USER_SIGN")
        assert(rs.next())
        assert(rs.getString("value").nonEmpty)
      }
    }

    withSessionConf()()() {
      checkSessionUser
    }

    withSessionConf(Map.empty)(Map.empty)(Map(s"${SESSION_USER_SIGN_ENABLED.key}" -> "false")) {
      checkSessionUser
    }

    withSessionConf(Map.empty)(Map(s"${SESSION_USER_SIGN_ENABLED.key}" -> "false"))(Map.empty) {
      checkSessionUser
    }

    withSessionConf(Map(s"${SESSION_USER_SIGN_ENABLED.key}" -> "false"))(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET $KYUUBI_SESSION_USER_SIGN")
        assert(rs.next())
        assert(rs.getString("value").nonEmpty)
      }
    }
  }

  test("KYUUBI #3839 session user sign - check session user sign") {
    withSessionConf()()() {
      withJdbcStatement() { statement =>
        val value = s"SET ${OPERATION_LANGUAGE.key}=scala"
        statement.executeQuery(value)

        val rs1 = statement.executeQuery(
          s"""
             |spark.sparkContext.getLocalProperty("$KYUUBI_SESSION_SIGN_PUBLICKEY")
             |""".stripMargin)
        assert(rs1.next())

        val rs2 = statement.executeQuery(
          s"""
             |spark.sparkContext.getLocalProperty("$KYUUBI_SESSION_USER_SIGN")
             |""".stripMargin)
        assert(rs2.next())

        // skipping prefix "res0: String = " of returned scala result
        val publicKeyStr = rs1.getString(1).substring(15)
        val sessionUserSign = rs2.getString(1).substring(15)

        assert(StringUtils.isNotBlank(publicKeyStr))
        assert(StringUtils.isNotBlank(sessionUserSign))
        assert(SignUtils.verifySignWithECDSA(user, sessionUserSign, publicKeyStr))
      }
    }
  }
}
