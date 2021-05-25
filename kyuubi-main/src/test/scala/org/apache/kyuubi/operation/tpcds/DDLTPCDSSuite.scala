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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.tags.ExtendedSQLTest

@ExtendedSQLTest
class DDLTPCDSSuite extends WithKyuubiServer with JDBCTestUtils with TPCDSHelper {
  override protected val conf: KyuubiConf = KyuubiConf()

  override protected def jdbcUrl: String = getJdbcUrl

  override def format: String = "hive OPTIONS(fileFormat='parquet')"

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
        val fields = if (tableDef.fields.head.isPartitionKey) {
          tableDef.fields.tail :+ tableDef.fields.head
        } else {
          tableDef.fields
        }
        fields.zipWithIndex.foreach { case (f, i) =>
          assert(meta.getColumnName(i + 1) === f.name)
        }
      }

    }
  }
}
