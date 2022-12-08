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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._

class JDBCMetadataDatasourceSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.DERBY.toString)
    .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.connectionTimeout", "3000")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.maximumPoolSize", "99")
    .set(s"$METADATA_STORE_JDBC_DATASOURCE_PREFIX.idleTimeout", "60000")
  new JDBCMetadataDatasource().initialize(conf)

  test("test jdbc datasource") {
    val conn = JDBCMetadataDatasource.hikariDataSource.get.getConnection
    assert(conn.isValid(10))
  }

}
