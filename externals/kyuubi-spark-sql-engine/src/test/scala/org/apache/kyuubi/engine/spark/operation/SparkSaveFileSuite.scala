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

package org.apache.kyuubi.engine.spark.operation

import org.apache.hadoop.fs.Path

import org.apache.kyuubi.config.KyuubiConf.{
  OPERATION_RESULT_SAVE_TO_FILE, OPERATION_RESULT_SAVE_TO_FILE_DIR,
  OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS, OPERATION_RESULT_SAVE_TO_FILE_MINSIZE
}
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SparkSaveFileSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = {
    Map(
      OPERATION_RESULT_SAVE_TO_FILE.key -> "true",
      OPERATION_RESULT_SAVE_TO_FILE_MINSIZE.key -> "100",
      OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS.key -> "100")
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  private def sessionHandle: String = {
    engine.backendService.sessionManager.allSessions().head.handle.identifier.toString
  }

  private def getOperationHandle: String = {
    val operationHandle =
      engine.backendService.sessionManager.operationManager.allOperations()
        .head.getHandle.identifier.toString
    s"$sessionHandle/$operationHandle"
  }

  test("test save file effect") {
    var enginePath: Path = new Path(kyuubiConf.get(OPERATION_RESULT_SAVE_TO_FILE_DIR))
    val sparkFileSystem = enginePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    var sessionPath: Path = enginePath
    withJdbcStatement("table1") { statement =>
      enginePath =
        engine.backendService.sessionManager.asInstanceOf[SparkSQLSessionManager]
          .getEngineResultSavePath()
      sessionPath = new Path(s"$enginePath/$sessionHandle")
      statement.asInstanceOf[KyuubiStatement].getConnection.getClientInfo
      // test save result skip command
      statement.executeQuery("create table table1 as select UUID() from range(1000)")
      var resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
      statement.executeQuery("show tables")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
      // test query save result
      val res = statement.executeQuery("select * from table1")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(sparkFileSystem.exists(resultPath))
      res.close()
      assert(!sparkFileSystem.exists(resultPath))
      // test rows number less than minRows
      statement.executeQuery("select * from table1 limit 10")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
    }
    // delete session path after session close
    assert(!sparkFileSystem.exists(sessionPath))
  }
}
