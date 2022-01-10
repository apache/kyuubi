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

package org.apache.kyuubi.engine.flink.operation

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.engine.flink.result.Constants
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.TABLE_TYPE
import org.apache.kyuubi.service.ServiceState._

class FlinkOperationSuite extends WithFlinkSQLEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map()

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/"

  ignore("release session if shared level is CONNECTION") {
    logger.info(s"jdbc url is $jdbcUrl")
    assert(engine.getServiceState == STARTED)
    withJdbcStatement() { _ => }
    eventually(Timeout(20.seconds)) {
      assert(engine.getServiceState == STOPPED)
    }
  }

  test("get catalogs for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString("catalogs") === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

  test("get table type for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = Constants.SUPPORTED_TABLE_TYPES.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("get tables") {
    val table = "table_1_test"
    val table_view = "table_1_test_view"

    withJdbcStatement(table) { statement =>
      statement.execute(
        s"""
           | create table $table (
           |  id int,
           |  name string,
           |  price double
           | ) with (
           |   'connector' = 'filesystem'
           | )
       """.stripMargin)

      statement.execute(
        s"""
           | create view ${table_view}
           | as select 1
       """.stripMargin)

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      assert(rs1.next())
      assert(rs1.getString(1) == table)
      assert(rs1.next())
      assert(rs1.getString(1) == table_view)

      // get table , table name like table%
      val rs2 = metaData.getTables(null, null, "table%", Array("TABLE"))
      assert(rs2.next())
      assert(rs2.getString(1) == table)
      assert(!rs2.next())

      // get view , view name like *
      val rs3 = metaData.getTables(null, "default_database", "*", Array("VIEW"))
      assert(rs3.next())
      assert(rs3.getString(1) == table_view)

      // get view , view name like *, schema pattern like default_%
      val rs4 = metaData.getTables(null, "default_%", "*", Array("VIEW"))
      assert(rs4.next())
      assert(rs4.getString(1) == table_view)

      // get view , view name like *, schema pattern like no_exists_%
      val rs5 = metaData.getTables(null, "no_exists_%", "*", Array("VIEW"))
      assert(!rs5.next())
    }
  }

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "tmp.hello")
    }
  }

  test("execute statement - select decimal") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1.2BD, 1.23BD ")
      assert(resultSet.next())
      assert(resultSet.getBigDecimal(1) === java.math.BigDecimal.valueOf(1.2))
      assert(resultSet.getBigDecimal(2) === java.math.BigDecimal.valueOf(1.23))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DECIMAL)
      assert(metaData.getColumnType(2) === java.sql.Types.DECIMAL)
      assert(metaData.getPrecision(1) == 2)
      assert(metaData.getPrecision(2) == 3)
      assert(metaData.getScale(1) == 1)
      assert(metaData.getScale(2) == 2)
    }
  }

  test("execute statement - show functions") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show functions")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "function name")
      assert(resultSet.next())
    }
  }

  test("execute statement - show databases") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show databases")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "database name")
      assert(resultSet.next())
      assert(resultSet.getString(1) == "default_database")
    }
  }

  test("execute statement - show tables") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show tables")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "table name")
      assert(!resultSet.next())
    }
  }

  test("execute statement - explain query") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("explain select 1")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "result")
      assert(resultSet.next())
    }
  }

  test("execute statement - create/alter/drop catalog") {
    // TODO: validate table results after FLINK-25558 is resolved
    withJdbcStatement()({ statement =>
      statement.executeQuery("create catalog cat_a with ('type'='generic_in_memory')")
      assert(statement.execute("drop catalog cat_a"))
    })
  }

  test("execute statement - create/alter/drop database") {
    // TODO: validate table results after FLINK-25558 is resolved
    withJdbcStatement()({ statement =>
      statement.executeQuery("create database db_a")
      assert(statement.execute("alter database db_a set ('k1' = 'v1')"))
      assert(statement.execute("drop database db_a"))
    })
  }

  test("execute statement - create/alter/drop table") {
    // TODO: validate table results after FLINK-25558 is resolved
    withJdbcStatement()({ statement =>
      statement.executeQuery("create table tbl_a (a string)")
      assert(statement.execute("alter table tbl_a rename to tbl_b"))
      assert(statement.execute("drop table tbl_b"))
    })
  }

  test("execute statement - create/alter/drop view") {
    // TODO: validate table results after FLINK-25558 is resolved
    withMultipleConnectionJdbcStatement()({ statement =>
      statement.executeQuery("create view view_a as select 1")
      assert(statement.execute("alter view view_a rename to view_b"))
      assert(statement.execute("drop view view_b"))
    })
  }

  ignore("execute statement - insert into") {
    // TODO: ignore temporally due to KYUUBI #1704
    withMultipleConnectionJdbcStatement()({ statement =>
      statement.executeQuery("create table tbl_a (a int) with ('connector' = 'blackhole')")
      statement.executeUpdate("insert into tbl_a select 1")
    })
  }
}
