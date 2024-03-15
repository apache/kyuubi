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

package org.apache.kyuubi.engine.trino.operation

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

import io.trino.client.ClientStandardTypes._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.trino.{TrinoQueryTests, TrinoStatement, WithTrinoEngine}
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class TrinoOperationSuite extends WithTrinoEngine with TrinoQueryTests {
  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_TRINO_CONNECTION_CATALOG.key -> "memory")

  // use default schema, do not set to 'default', since withSessionHandle strip suffix '/;'
  override protected val schema = ""

  override protected def jdbcUrl: String = getJdbcUrl

  private val standardTypes: Set[String] = Set(
    BIGINT,
    INTEGER,
    SMALLINT,
    TINYINT,
    BOOLEAN,
    DATE,
    DECIMAL,
    REAL,
    DOUBLE,
    HYPER_LOG_LOG,
    QDIGEST,
    P4_HYPER_LOG_LOG,
    INTERVAL_DAY_TO_SECOND,
    INTERVAL_YEAR_TO_MONTH,
    TIMESTAMP,
    TIMESTAMP_WITH_TIME_ZONE,
    TIME,
    TIME_WITH_TIME_ZONE,
    VARBINARY,
    VARCHAR,
    CHAR,
    ROW,
    ARRAY,
    MAP,
    JSON,
    IPADDRESS,
    UUID,
    GEOMETRY,
    SPHERICAL_GEOGRAPHY,
    BING_TILE)

  test("trino - get type info") {
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo
      val expectedTypes = standardTypes ++ Set(
        "color",
        "KdbTree",
        "CodePoints",
        "JsonPath",
        "Regressor",
        "JoniRegExp",
        "unknown",
        "ObjectId",
        "SetDigest",
        "Re2JRegExp",
        "Model",
        "tdigest",
        "LikePattern",
        "function",
        "Classifier",
        "json2016",
        "JsonPath2016")
      val typeInfos: Set[String] = Set()
      while (typeInfo.next()) {
        assert(expectedTypes.contains(typeInfo.getString(TYPE_NAME)))
        typeInfos += typeInfo.getString(TYPE_NAME)
      }
      assert(expectedTypes.size === typeInfos.size)
    }
  }

  test("trino - get catalogs") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val resultSetBuffer = ArrayBuffer[String]()
      while (catalogs.next()) {
        resultSetBuffer += catalogs.getString(TABLE_CAT)
      }
      assert(resultSetBuffer.contains("memory"))
      assert(resultSetBuffer.contains("system"))
    }
  }

  test("trino - get table types") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = Set("TABLE", "VIEW").toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("trino - get tables") {
    case class TableWithCatalogAndSchema(
        catalog: String,
        schema: String,
        tableName: String,
        tableType: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultSetBuffer = ArrayBuffer[TableWithCatalogAndSchema]()

      var tables = meta.getTables(null, null, null, null)
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "system",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "information_schema",
        "tables",
        "TABLE")))

      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test_escape_1")
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test1escape_1")
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test_escape11")
      statement.execute("CREATE TABLE IF NOT EXISTS memory.test_escape_1.test_escape_1(a varchar)")
      statement.execute("CREATE TABLE IF NOT EXISTS memory.test1escape_1.test1escape_1(a varchar)")
      statement.execute("CREATE TABLE IF NOT EXISTS memory.test_escape11.test_escape11(a varchar)")
      statement.execute(
        """
          |CREATE OR REPLACE VIEW memory.test_escape_1.test_view AS
          |SELECT  * FROM memory.test_escape_1.test_escape_1
          |""".stripMargin)

      tables = meta.getTables(null, null, null, null)
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "system",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test1escape_1",
        "test1escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape11",
        "test_escape11",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_view",
        "VIEW")))

      tables = meta.getTables("memory", null, null, null)
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test1escape_1",
        "test1escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape11",
        "test_escape11",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_view",
        "VIEW")))

      tables = meta.getTables(null, "test%", null, null)
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test1escape_1",
        "test1escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape11",
        "test_escape11",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_view",
        "VIEW")))

      tables = meta.getTables(null, null, "test_%", null)
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape11",
        "test_escape11",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_view",
        "VIEW")))

      tables = meta.getTables(null, null, null, Array("TABLE"))
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "system",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "information_schema",
        "tables",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test1escape_1",
        "test1escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape11",
        "test_escape11",
        "TABLE")))

      tables = meta.getTables(null, null, "test_escape\\_1", null)
      resultSetBuffer.clear()
      while (tables.next()) {
        resultSetBuffer +=
          TableWithCatalogAndSchema(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test_escape_1",
        "test_escape_1",
        "TABLE")))
      assert(resultSetBuffer.contains(TableWithCatalogAndSchema(
        "memory",
        "test1escape_1",
        "test1escape_1",
        "TABLE")))

      statement.execute("DROP VIEW memory.test_escape_1.test_view")
      statement.execute("DROP TABLE memory.test_escape_1.test_escape_1")
      statement.execute("DROP TABLE memory.test1escape_1.test1escape_1")
      statement.execute("DROP TABLE memory.test_escape11.test_escape11")
      statement.execute("DROP SCHEMA memory.test_escape_1")
      statement.execute("DROP SCHEMA memory.test1escape_1")
      statement.execute("DROP SCHEMA memory.test_escape11")
    }
  }

  test("trino - get schemas") {
    case class SchemaWithCatalog(catalog: String, schema: String)

    withJdbcStatement() { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test_escape_1")
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test2escape_1")
      statement.execute("CREATE SCHEMA IF NOT EXISTS memory.test_escape11")

      val meta = statement.getConnection.getMetaData
      val resultSetBuffer = ArrayBuffer[SchemaWithCatalog]()

      val schemas1 = meta.getSchemas(null, null)
      while (schemas1.next()) {
        resultSetBuffer +=
          SchemaWithCatalog(schemas1.getString(TABLE_CATALOG), schemas1.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(SchemaWithCatalog("memory", "information_schema")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("system", "information_schema")))

      val schemas2 = meta.getSchemas("memory", null)
      resultSetBuffer.clear()
      while (schemas2.next()) {
        resultSetBuffer +=
          SchemaWithCatalog(schemas2.getString(TABLE_CATALOG), schemas2.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(SchemaWithCatalog("memory", "default")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("memory", "information_schema")))
      assert(!resultSetBuffer.exists(f => f.catalog == "system"))

      val schemas3 = meta.getSchemas(null, "sf_")
      resultSetBuffer.clear()
      while (schemas3.next()) {
        resultSetBuffer +=
          SchemaWithCatalog(schemas3.getString(TABLE_CATALOG), schemas3.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf1")))
      assert(!resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf10")))

      val schemas4 = meta.getSchemas(null, "sf%")
      resultSetBuffer.clear()
      while (schemas4.next()) {
        resultSetBuffer +=
          SchemaWithCatalog(schemas4.getString(TABLE_CATALOG), schemas4.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf1")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf10")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf100")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("tpcds", "sf1000")))

      // test escape the second '_'
      val schemas5 = meta.getSchemas("memory", "test_escape\\_1")
      resultSetBuffer.clear()
      while (schemas5.next()) {
        resultSetBuffer +=
          SchemaWithCatalog(schemas5.getString(TABLE_CATALOG), schemas5.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(SchemaWithCatalog("memory", "test_escape_1")))
      assert(resultSetBuffer.contains(SchemaWithCatalog("memory", "test2escape_1")))
      assert(!resultSetBuffer.contains(SchemaWithCatalog("memory", "test_escape11")))

      statement.execute("DROP SCHEMA memory.test_escape_1")
      statement.execute("DROP SCHEMA memory.test2escape_1")
      statement.execute("DROP SCHEMA memory.test_escape11")
    }
  }

  test("trino - get columns") {
    case class ColumnWithTableAndCatalogAndSchema(
        catalog: String,
        schema: String,
        tableName: String,
        columnName: String,
        typeName: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultSetBuffer = ArrayBuffer[ColumnWithTableAndCatalogAndSchema]()

      var columns = meta.getColumns(null, null, null, null)
      while (columns.next()) {
        resultSetBuffer +=
          ColumnWithTableAndCatalogAndSchema(
            columns.getString(TABLE_CAT),
            columns.getString(TABLE_SCHEM),
            columns.getString(TABLE_NAME),
            columns.getString(COLUMN_NAME),
            columns.getString(TYPE_NAME))
      }
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "table_catalog",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "table_schema",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "table_name",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "column_name",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "ordinal_position",
        BIGINT)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "column_default",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "is_nullable",
        VARCHAR)))
      assert(resultSetBuffer.contains(ColumnWithTableAndCatalogAndSchema(
        "memory",
        "information_schema",
        "columns",
        "data_type",
        VARCHAR)))

      val columnTypes = standardTypes.map {
        case ARRAY => s"$ARRAY($VARCHAR)"
        case MAP => s"$MAP($VARCHAR, $VARCHAR)"
        case ROW => s"$ROW(c $VARCHAR)"
        case QDIGEST => s"$QDIGEST($VARCHAR)"
        case columnType => columnType
      }
      var schema: Seq[String] = Seq()
      for (position <- 0 until columnTypes.size) {
        schema = schema :+ s"c$position ${columnTypes.toSeq(position)}"
      }
      statement.execute(s"CREATE SCHEMA IF NOT EXISTS memory.test_schema")
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS memory.test_schema.test_column(${schema.mkString(",")})")

      columns = meta.getColumns("memory", "test_schema", "test_column", null)

      var position = 0
      while (columns.next()) {
        assert(columns.getString(TABLE_CAT) === "memory")
        assert(columns.getString(TABLE_SCHEM) === "test_schema")
        assert(columns.getString(TABLE_NAME) === "test_column")
        assert(columns.getString(COLUMN_NAME) === s"c$position")

        val expectType = columnTypes.toSeq(position) match {
          case CHAR => s"$CHAR(1)"
          case DECIMAL => s"$DECIMAL(38,0)"
          case TIME => s"$TIME(3)"
          case TIME_WITH_TIME_ZONE => s"$TIME(3) with time zone"
          case TIMESTAMP => s"$TIMESTAMP(3)"
          case TIMESTAMP_WITH_TIME_ZONE => s"$TIMESTAMP(3) with time zone"
          case columnType => columnType
        }
        assert(columns.getString(TYPE_NAME) === expectType)
        position += 1
      }
      assert(position === columnTypes.size, "all columns should have been verified")

      statement.execute("DROP TABLE memory.test_schema.test_column")
      statement.execute("DROP SCHEMA memory.test_schema")
    }
  }

  test("trino - get functions") {
    withJdbcStatement() { statement =>
      val exceptionMsg = intercept[Exception](statement.getConnection.getMetaData.getFunctions(
        null,
        null,
        "abs")).getMessage
      assert(exceptionMsg === KyuubiSQLException.featureNotSupported().getMessage)
    }
  }

  test("execute statement -  select decimal") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT DECIMAL '1.2' as col1, DECIMAL '1.23' AS col2")
      assert(resultSet.next())
      assert(resultSet.getBigDecimal("col1") === new java.math.BigDecimal("1.2"))
      assert(resultSet.getBigDecimal("col2") === new java.math.BigDecimal("1.23"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DECIMAL)
      assert(metaData.getColumnType(2) === java.sql.Types.DECIMAL)
      assert(metaData.getPrecision(1) == 2)
      assert(metaData.getPrecision(2) == 3)
      assert(metaData.getScale(1) == 1)
      assert(metaData.getScale(2) == 2)
    }
  }

  test("test fetch orientation") {
    val sql = "SELECT id FROM (VALUES 0, 1) as t(id)"

    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      waitForOperationToComplete(client, opHandle)

      // fetch next from before first row
      val tFetchResultsReq1 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1)
      val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq1)
      assert(tFetchResultsResp1.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val idSeq1 = tFetchResultsResp1.getResults.getColumns.get(0).getI32Val.getValues.asScala
      assertResult(Seq(0L))(idSeq1)

      // fetch next from first row
      val tFetchResultsReq2 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1)
      val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
      assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val idSeq2 = tFetchResultsResp2.getResults.getColumns.get(0).getI32Val.getValues.asScala
      assertResult(Seq(1L))(idSeq2)

      val tFetchResultsReq3 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_PRIOR, 1)
      val tFetchResultsResp3 = client.FetchResults(tFetchResultsReq3)
      if (kyuubiConf.get(ENGINE_TRINO_OPERATION_INCREMENTAL_COLLECT)) {
        assert(tFetchResultsResp3.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      } else {
        assert(tFetchResultsResp3.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq3 =
          tFetchResultsResp3.getResults.getColumns.get(0).getI32Val.getValues.asScala
        assertResult(Seq(0L))(idSeq3)
      }

      val tFetchResultsReq4 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_FIRST, 3)
      val tFetchResultsResp4 = client.FetchResults(tFetchResultsReq4)
      if (kyuubiConf.get(ENGINE_TRINO_OPERATION_INCREMENTAL_COLLECT)) {
        assert(tFetchResultsResp4.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      } else {
        assert(tFetchResultsResp4.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq4 =
          tFetchResultsResp4.getResults.getColumns.get(0).getI32Val.getValues.asScala
        assertResult(Seq(0L, 1L))(idSeq4)
      }
    }
  }

  test("get operation status") {
    val sql = "select date '2011-11-11' - interval '1' day"

    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      val tGetOperationStatusReq = new TGetOperationStatusReq()
      tGetOperationStatusReq.setOperationHandle(opHandle)
      val resp = client.GetOperationStatus(tGetOperationStatusReq)
      val status = resp.getStatus
      assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(resp.getOperationState === TOperationState.FINISHED_STATE)
      assert(resp.isHasResultSet)
    }
  }

  test("basic open | execute | close") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("hongdd")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setRunAsync(true)
      tExecuteStatementReq.setStatement("show session")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val operationHandle = tExecuteStatementResp.getOperationHandle
      waitForOperationToComplete(client, operationHandle)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(1)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      val logs = tFetchResultsResp.getResults.getColumns.get(0).getStringVal.getValues.asScala
      assert(logs.exists(_.contains(classOf[ExecuteStatement].getCanonicalName)))

      tFetchResultsReq.setFetchType(0)
      val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq)
      val rs = tFetchResultsResp1.getResults.getColumns.get(0).getStringVal.getValues.asScala
      assert(rs.contains("aggregation_operator_unspill_memory_limit"))

      val tCloseSessionReq = new TCloseSessionReq()
      tCloseSessionReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      val tCloseSessionResp = client.CloseSession(tCloseSessionReq)
      assert(tCloseSessionResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("not allow to operate closed session or operation") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("hongdd")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("show session")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val tCloseOperationReq = new TCloseOperationReq(tExecuteStatementResp.getOperationHandle)
      val tCloseOperationResp = client.CloseOperation(tCloseOperationReq)
      assert(tCloseOperationResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(tFetchResultsResp.getStatus.getErrorMessage startsWith "Invalid OperationHandle [")

      val tCloseSessionReq = new TCloseSessionReq()
      tCloseSessionReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      val tCloseSessionResp = client.CloseSession(tCloseSessionReq)
      assert(tCloseSessionResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val tExecuteStatementResp1 = client.ExecuteStatement(tExecuteStatementReq)

      val status = tExecuteStatementResp1.getStatus
      assert(status.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(status.getErrorMessage startsWith s"Invalid SessionHandle [")
    }
  }

  test("cancel operation") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("hongdd")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("show session")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)
      val tCancelOperationReq = new TCancelOperationReq(tExecuteStatementResp.getOperationHandle)
      val tCancelOperationResp = client.CancelOperation(tCancelOperationReq)
      assert(tCancelOperationResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("trino - set/get catalog with session conf") {
    Seq(true, false).foreach { enable =>
      withSessionConf()(
        Map(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> enable.toString))(Map.empty) {
        withJdbcStatement() { statement =>
          val catalog = statement.getConnection.getCatalog
          if (enable) {
            assert(catalog == "memory")
          } else {
            assert(catalog == "")
          }
          statement.getConnection.setCatalog("system")
          val changedCatalog = statement.getConnection.getCatalog
          if (enable) {
            assert(changedCatalog == "system")
          } else {
            assert(changedCatalog == "")
          }
        }
      }
    }
  }

  test("[KYUUBI #3452] Implement GetInfo for Trino engine") {
    def getTrinoVersion: String = {
      var version: String = "Unknown"
      withTrinoContainer { trinoContext =>
        val trinoStatement = TrinoStatement(trinoContext, kyuubiConf, "SELECT version()")
        val schema = trinoStatement.getColumns
        val resultSet = trinoStatement.execute()

        assert(schema.size === 1)
        assert(schema(0).getName === "_col0")

        assert(resultSet.hasNext)
        version = resultSet.next().head.toString
      }
      version
    }

    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Trino")

        val req2 = new TGetInfoReq()
        req2.setSessionHandle(handle)
        req2.setInfoType(TGetInfoType.CLI_DBMS_VER)
        assert(client.GetInfo(req2).getInfoValue.getStringValue === getTrinoVersion)

        val req3 = new TGetInfoReq()
        req3.setSessionHandle(handle)
        req3.setInfoType(TGetInfoType.CLI_MAX_COLUMN_NAME_LEN)
        assert(client.GetInfo(req3).getInfoValue.getLenValue === 0)
      }
    }
  }
}
