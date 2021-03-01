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

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.{SparkSQLEngine, WithSparkSQLEngine}
import org.apache.kyuubi.operation.JDBCTestUtils

class SessionSuite extends WithSparkSQLEngine with JDBCTestUtils {
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val warehousePath = Utils.createTempDir()
    val metastorePath = Utils.createTempDir()
    warehousePath.toFile.delete()
    metastorePath.toFile.delete()
    System.setProperty("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=$metastorePath;create=true")
    System.setProperty("spark.sql.warehouse.dir", warehousePath.toString)
    System.setProperty("spark.sql.hive.metastore.sharedPrefixes", "org.apache.hive.jdbc")
    System.setProperty(ENGINE_SHARED_LEVEL.key, "CONNECTION")
    spark = SparkSQLEngine.createSpark()
    engine = SparkSQLEngine.startEngine(spark)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (engine != null) {
      engine.stop()
      engine = null
    }
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  override protected def jdbcUrl: String = s"jdbc:hive2://${engine.connectionUrl}/;"

  test("release session if shared level is CONNECTION") {
    assert(engine.started.get)
    withJdbcStatement() {_ => }
    eventually(Timeout(60.seconds)) {
      assert(!engine.started.get)
    }
  }
}
