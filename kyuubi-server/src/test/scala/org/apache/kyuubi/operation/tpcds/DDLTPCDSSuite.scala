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

package org.apache.kyuubi.operation.tpcds

import org.apache.kyuubi.{DeltaSuiteMixin, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.tags.{DeltaTest, ExtendedSQLTest}

@DeltaTest
@ExtendedSQLTest
class DDLTPCDSSuite extends WithKyuubiServer
  with JDBCTestUtils
  with TPCDSHelper
  with DeltaSuiteMixin {

  override protected val conf: KyuubiConf = {
    val kyuubiConf = KyuubiConf().set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 20000L)
     extraConfigs.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override protected def jdbcUrl: String = getJdbcUrl

  override def database: String = this.getClass.getSimpleName

  override def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE DATABASE IF NOT EXISTS $database")
    }
  }

  override def afterAll(): Unit = {
    withJdbcStatement() { statement =>
      statement.execute(s"DROP DATABASE IF EXISTS $database")
    }
    super.afterAll()
  }

  tables.foreach { tableDef =>
    test(s"CREATE ${tableDef.table.qualifiedName}") {
      withJdbcStatement(tableDef.table.qualifiedName) { statement =>
        statement.execute(tableDef.create)
        val resultSet = statement.executeQuery(s"SELECT * FROM ${tableDef.table.qualifiedName}")
        assert(!resultSet.next())
        val meta = resultSet.getMetaData
        tableDef.fields.zipWithIndex.foreach { case (f, i) =>
          assert(meta.getColumnName(i + 1) === f.name)
        }
      }

    }
  }
}
