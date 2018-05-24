/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation

import scala.collection.JavaConverters._

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach

import yaooqinn.kyuubi.cli.FetchOrientation.FETCH_NEXT
import yaooqinn.kyuubi.schema.ColumnBasedSet
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.session.{KyuubiSession, SessionManager}
import yaooqinn.kyuubi.spark.SparkSessionWithUGI
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiOperationSuite extends SparkFunSuite with BeforeAndAfterEach {

  val conf = new SparkConf(loadDefaults = true).setAppName("operation test")
  KyuubiServer.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local")
  var sessionMgr: SessionManager = _
  val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
  val user = UserGroupInformation.getCurrentUser
  val userName = user.getShortUserName
  val passwd = ""
  val statement = "show tables"
  var session: KyuubiSession = _
  var spark: SparkSession = _
  var sparkWithUgi: SparkSessionWithUGI = _

  override protected def beforeAll(): Unit = {
    val sc = ReflectUtils
      .newInstance(classOf[SparkContext].getName, Seq(classOf[SparkConf]), Seq(conf))
      .asInstanceOf[SparkContext]
    spark = ReflectUtils.newInstance(
      classOf[SparkSession].getName,
      Seq(classOf[SparkContext]),
      Seq(sc)).asInstanceOf[SparkSession]
    sparkWithUgi = new SparkSessionWithUGI(user, conf)
    ReflectUtils.setFieldValue(sparkWithUgi,
      "yaooqinn$kyuubi$spark$SparkSessionWithUGI$$_sparkSession", spark)
    sessionMgr = new SessionManager()
    sessionMgr.init(conf)
    sessionMgr.start()
    session = new KyuubiSession(
      proto, userName, passwd, conf, "", false, sessionMgr, sessionMgr.getOperationMgr)
    ReflectUtils.setFieldValue(session, "sparkSessionWithUGI", sparkWithUgi)
  }

  protected override def afterAll(): Unit = {
    session.close()
    session = null
    sessionMgr.stop()
    spark.stop()
  }

  test("testCancel") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.cancel()
    assert(op.getStatus.getState === CANCELED)
  }

  test("testGetHandle") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.getHandle.isHasResultSet)
    assert(!op.getHandle.toTOperationHandle.isHasResultSet)
    op.getHandle.setHasResultSet(true)
    assert(op.getHandle.isHasResultSet)
    assert(op.getHandle.toTOperationHandle.isHasResultSet)
    assert(op.getHandle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(op.getHandle.getOperationType === EXECUTE_STATEMENT)
  }

  test("testGetStatus") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    assert(op.getStatus.getOperationException === null)
  }

  test("testIsTimedOut") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.isTimedOut)
  }

  test("testGetNextRowSet") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val ds = spark.sql(statement)
    ReflectUtils.invokeMethod(op, "setState", Seq(classOf[OperationState]), Seq(RUNNING))
    ReflectUtils.invokeMethod(op, "setState", Seq(classOf[OperationState]), Seq(FINISHED))
    ReflectUtils.setFieldValue(op,
      "iter", ds.toLocalIterator().asScala)
    val rowSet = op.getNextRowSet(FETCH_NEXT, 10)
    assert(rowSet.isInstanceOf[ColumnBasedSet])
  }

  test("testGetProtocolVersion") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getProtocolVersion === proto)
  }

  test("testGetOperationLog") {
    // TODO
  }

  test("testClose") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.close()
    assert(op.getStatus.getState === CLOSED)
  }

  test("testGetSession") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val s = op.getSession
    assert(s.sparkSession === spark)
    assert(s == session)
    assert(s.getUserName === userName)
  }

  test("testDEFAULT_FETCH_ORIENTATION") {
    assert(KyuubiOperation.DEFAULT_FETCH_ORIENTATION === FETCH_NEXT)
  }

  test("testDEFAULT_FETCH_MAX_ROWS") {
    assert(KyuubiOperation.DEFAULT_FETCH_MAX_ROWS === 100)
  }
}
