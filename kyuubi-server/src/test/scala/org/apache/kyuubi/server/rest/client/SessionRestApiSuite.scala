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

package org.apache.kyuubi.server.rest.client

import java.util
import java.util.Collections

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.client.{KyuubiRestClient, SessionRestApi}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.plugin.SessionConfAdvisor
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TGetInfoType

class SessionRestApiSuite extends RestClientTestHelper {
  override protected lazy val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.SESSION_CONF_ADVISOR, Seq(classOf[TestSessionConfAdvisor].getName))

  test("get/close/list/count session") {
    withSessionRestApi { sessionRestApi =>
      {
        // open session
        val sessionOpenRequest = new SessionOpenRequest(Map("testConfig" -> "testValue").asJava)
        sessionRestApi.openSession(sessionOpenRequest)

        // list sessions
        var sessions = sessionRestApi.listSessions().asScala
        assert(sessions.size == 1)
        val sessionHandle = sessions(0).getIdentifier

        // get open session count
        var sessionCount = sessionRestApi.getOpenSessionCount
        assert(sessionCount == 1)

        // close session
        sessionRestApi.closeSession(sessionHandle)

        // list sessions again
        sessions = sessionRestApi.listSessions().asScala
        assert(sessions.isEmpty)

        // get open session count again
        sessionCount = sessionRestApi.getOpenSessionCount
        assert(sessionCount == 0)
      }
    }
  }

  test("get session event") {
    withSessionRestApi { sessionRestApi =>
      // open session
      val sessionOpenRequest = new SessionOpenRequest(Map("testConfig" -> "testValue").asJava)
      val sessionHandle = sessionRestApi.openSession(sessionOpenRequest)

      // get session event
      val kyuubiEvent = sessionRestApi.getSessionEvent(
        sessionHandle.getIdentifier.toString)
      assert(kyuubiEvent.getConf.get("testConfig").equals("testValue"))
      assert(kyuubiEvent.getConf.get("spark.optimized") == null)
      assert(kyuubiEvent.getOptimizedConf.get("spark.optimized").equals("true"))
      assert(kyuubiEvent.getSessionType.equals(SessionType.INTERACTIVE.toString))
    }
  }

  test("get info type") {
    withSessionRestApi { sessionRestApi =>
      // open session
      val sessionOpenRequest = new SessionOpenRequest(
        Map("testConfig" -> "testValue", KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER").asJava)
      val sessionHandle = sessionRestApi.openSession(sessionOpenRequest)

      // get session info
      val info = sessionRestApi.getSessionInfo(
        sessionHandle.getIdentifier.toString,
        TGetInfoType.CLI_SERVER_NAME.getValue)
      assert(info.getInfoType.equals("CLI_SERVER_NAME"))
      assert(info.getInfoValue.equals("Apache Kyuubi"))
    }
  }

  test("submit operation") {
    withSessionRestApi { sessionRestApi =>
      // open session
      val sessionOpenRequest = new SessionOpenRequest(Map("testConfig" -> "testValue").asJava)
      val sessionHandle = sessionRestApi.openSession(sessionOpenRequest)
      val sessionHandleStr = sessionHandle.getIdentifier.toString

      // execute statement
      val op1 = sessionRestApi.executeStatement(
        sessionHandleStr,
        new StatementRequest("show tables", true, 3000))
      assert(op1.getIdentifier != null)

      // get type info
      val op2 = sessionRestApi.getTypeInfo(sessionHandleStr)
      assert(op2.getIdentifier != null)

      // get catalogs
      val op3 = sessionRestApi.getCatalogs(sessionHandleStr)
      assert(op3.getIdentifier != null)

      // get schemas
      val op4 = sessionRestApi.getSchemas(
        sessionHandleStr,
        new GetSchemasRequest("spark_catalog", "default"))
      assert(op4.getIdentifier != null)

      // get tables
      val tableTypes = new util.ArrayList[String]()
      val op5 = sessionRestApi.getTables(
        sessionHandleStr,
        new GetTablesRequest("spark_catalog", "default", "default", tableTypes))
      assert(op5.getIdentifier != null)

      // get table types
      val op6 = sessionRestApi.getTableTypes(sessionHandleStr)
      assert(op6.getIdentifier != null)

      // get columns
      val op7 = sessionRestApi.getColumns(
        sessionHandleStr,
        new GetColumnsRequest("spark_catalog", "default", "default", "default"))
      assert(op7.getIdentifier != null)

      // get function
      val op8 = sessionRestApi.getFunctions(
        sessionHandleStr,
        new GetFunctionsRequest("default", "default", "default"))
      assert(op8.getIdentifier != null)

      // get primary keys
      assertThrows[KyuubiRestException] {
        sessionRestApi.getPrimaryKeys(
          sessionHandleStr,
          new GetPrimaryKeysRequest("spark_catalog", "default", "default"))
      }

      // get cross reference
      val getCrossReferenceReq = new GetCrossReferenceRequest(
        "spark_catalog",
        "default",
        "default",
        "spark_catalog",
        "default",
        "default")
      assertThrows[KyuubiRestException] {
        sessionRestApi.getCrossReference(sessionHandleStr, getCrossReferenceReq)
      }
    }
  }

  test("fix kyuubi session leak caused by engine stop") {
    withSessionRestApi { sessionRestApi =>
      // close all sessions
      val sessions = sessionRestApi.listSessions().asScala
      sessions.foreach(session => sessionRestApi.closeSession(session.getIdentifier))

      // open new session
      val sessionOpenRequest = new SessionOpenRequest(Map(
        KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED.key -> "true",
        KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL.key -> "5000",
        KyuubiConf.ENGINE_ALIVE_TIMEOUT.key -> "3000").asJava)
      val sessionHandle = sessionRestApi.openSession(sessionOpenRequest)

      // get open session count
      val sessionCount = sessionRestApi.getOpenSessionCount
      assert(sessionCount == 1)

      val statementReq = new StatementRequest(
        "spark.stop()",
        true,
        3000,
        Collections.singletonMap(KyuubiConf.OPERATION_LANGUAGE.key, "SCALA"))
      sessionRestApi.executeStatement(sessionHandle.getIdentifier.toString, statementReq)

      eventually(Timeout(30.seconds), interval(1.seconds)) {
        assert(sessionRestApi.getOpenSessionCount == 0)
        assert(sessionRestApi.listSessions().asScala.isEmpty)
      }
    }
  }

  def withSessionRestApi[T](f: SessionRestApi => T): T = {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val sessionRestApi = new SessionRestApi(basicKyuubiRestClient)
    f(sessionRestApi)
  }
}

class TestSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfOverlay(
      user: String,
      sessionConf: java.util.Map[String, String]): java.util.Map[String, String] = {
    Map("spark.optimized" -> "true").asJava
  }
}
