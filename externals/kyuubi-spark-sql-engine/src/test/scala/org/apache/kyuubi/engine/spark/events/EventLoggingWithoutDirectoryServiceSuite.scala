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

package org.apache.kyuubi.engine.spark.events


import java.io.IOException

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.events.EventLoggerType._
import org.apache.kyuubi.operation.JDBCTestUtils

class EventLoggingWithoutDirectoryServiceSuite extends WithSparkSQLEngine with JDBCTestUtils {

  private val logRoot = "file:///tmp/aaa/bbb"
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  override def withKyuubiConf: Map[String, String] = Map(
    KyuubiConf.ENGINE_EVENT_LOGGERS.key -> s"$JSON,$SPARK",
    KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH.key -> logRoot
  )

  override protected def jdbcUrl: String = getJdbcUrl

  private var exception: IOException = _
  override def startSparkEngine(): Unit = {
    try {
      super.startSparkEngine()
    } catch {
      case e: IOException =>
        exception = e
    }
  }

  test("get IOException when this file is not exist") {
    assert(exception.isInstanceOf[IOException])
  }

}
