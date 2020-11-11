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

import java.sql.{DatabaseMetaData, Date, ResultSet, SQLException, SQLFeatureNotSupportedException, Timestamp}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.rpc.thrift._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

class SparkOperationSuite extends WithSparkSQLEngine {

  test("get table types") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = CatalogTableType.tableTypes.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next().name)
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("get columns operation") {
    val tableName = "spark_get_col_operation"
    val schema = new StructType()
      .add("c0", "boolean", nullable = false, "0")
      .add("c1", "tinyint", nullable = true, "1")
      .add("c2", "smallint", nullable = false, "2")
      .add("c3", "int", nullable = true, "3")
      .add("c4", "long", nullable = false, "4")
      .add("c5", "float", nullable = true, "5")
      .add("c6", "double", nullable = false, "6")
      .add("c7", "decimal(38, 20)", nullable = true, "7")
      .add("c8", "decimal(10, 2)", nullable = false, "8")
      .add("c9", "string", nullable = true, "9")
      .add("c10", "array<long>", nullable = false, "10")
      .add("c11", "array<string>", nullable = true, "11")
      .add("c12", "map<smallint, tinyint>", nullable = false, "12")
      .add("c13", "date", nullable = true, "13")
      .add("c14", "timestamp", nullable = false, "14")
      .add("c15", "struct<X: bigint,Y: double>", nullable = true, "15")
      .add("c16", "binary", nullable = false, "16")
      .add("c17", "struct<X: string>", true, "17")

    val ddl =
      s"""
         |CREATE TABLE IF NOT EXISTS $dftSchema.$tableName (
         |  ${schema.toDDL}
         |)
         |using parquet""".stripMargin

    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val metaData = statement.getConnection.getMetaData

      Seq("%", null, ".*", "c.*") foreach { pattern =>
        val rowSet = metaData.getColumns("", dftSchema, tableName, pattern)

        import java.sql.Types._
        val expectedJavaTypes = Seq(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
          DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY,
          STRUCT)

        var pos = 0

        while (rowSet.next()) {
          assert(rowSet.getString(TABLE_CAT) === null)
          assert(rowSet.getString(TABLE_SCHEM) === dftSchema)
          assert(rowSet.getString(TABLE_NAME) === tableName)
          assert(rowSet.getString(COLUMN_NAME) === schema(pos).name)
          assert(rowSet.getInt(DATA_TYPE) === expectedJavaTypes(pos))
          assert(rowSet.getString(TYPE_NAME) === schema(pos).dataType.sql)

          val colSize = rowSet.getInt(COLUMN_SIZE)
          schema(pos).dataType match {
            case StringType | BinaryType | _: ArrayType | _: MapType => assert(colSize === 0)
            case StructType(fields) if fields.length == 1 => assert(colSize === 0)
            case o => assert(colSize === o.defaultSize)
          }

          assert(rowSet.getInt(BUFFER_LENGTH) === 0) // not used
          val decimalDigits = rowSet.getInt(DECIMAL_DIGITS)
          schema(pos).dataType match {
            case BooleanType | _: IntegerType => assert(decimalDigits === 0)
            case d: DecimalType => assert(decimalDigits === d.scale)
            case FloatType => assert(decimalDigits === 7)
            case DoubleType => assert(decimalDigits === 15)
            case TimestampType => assert(decimalDigits === 6)
            case _ => assert(decimalDigits === 0) // nulls
          }

          val radix = rowSet.getInt(NUM_PREC_RADIX)
          schema(pos).dataType match {
            case _: NumericType => assert(radix === 10)
            case _ => assert(radix === 0) // nulls
          }

          assert(rowSet.getInt(NULLABLE) === 1)
          assert(rowSet.getString(REMARKS) === pos.toString)
          assert(rowSet.getInt(ORDINAL_POSITION) === pos)
          assert(rowSet.getString(IS_NULLABLE) === "YES")
          assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
          pos += 1
        }

        assert(pos === 18, "all columns should have been verified")
      }

      val e = intercept[HiveSQLException](metaData.getColumns(null, "*", null, null))
      assert(e.getCause.getMessage === "org.apache.kyuubi.KyuubiSQLException:" +
        "Error operating GET_COLUMNS: Dangling meta character '*' near index 0\n*\n^")

      val e1 = intercept[HiveSQLException](metaData.getColumns(null, null, null, "*"))
      assert(e1.getCause.getMessage === "org.apache.kyuubi.KyuubiSQLException:" +
        "Error operating GET_COLUMNS: Dangling meta character '*' near index 0\n*\n^")
    }
  }

  test("get columns operation should handle interval column properly") {
    val viewName = "view_interval"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select interval 1 day as i"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, null)
      while (rowSet.next()) {
        assert(rowSet.getString(TABLE_CAT) === null)
        assert(rowSet.getString(TABLE_SCHEM) === "global_temp")
        assert(rowSet.getString(TABLE_NAME) === viewName)
        assert(rowSet.getString(COLUMN_NAME) === "i")
        assert(rowSet.getInt(DATA_TYPE) === java.sql.Types.OTHER)
        assert(rowSet.getString(TYPE_NAME).equalsIgnoreCase(CalendarIntervalType.sql))
        assert(rowSet.getInt(COLUMN_SIZE) === CalendarIntervalType.defaultSize)
        assert(rowSet.getInt(DECIMAL_DIGITS) === 0)
        assert(rowSet.getInt(NUM_PREC_RADIX) === 0)
        assert(rowSet.getInt(NULLABLE) === 0)
        assert(rowSet.getString(REMARKS) === "")
        assert(rowSet.getInt(ORDINAL_POSITION) === 0)
        assert(rowSet.getString(IS_NULLABLE) === "YES")
        assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
      }
    }
  }

  test("handling null in view for get columns operations") {
    val viewName = "view_null"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select null as n"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, "n")
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName)
        assert(rowSet.getString("COLUMN_NAME") === "n")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.NULL)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(NullType.sql))
        assert(rowSet.getInt("COLUMN_SIZE") === 1)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 1)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }
  }

  test("get functions") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val apis = Seq(metaData.getFunctions _, metaData.getProcedures _)
      Seq("to_timestamp", "date_part", "lpad", "date_format", "cos", "sin").foreach { func =>
        apis.foreach { apiFunc =>
          val resultSet = apiFunc("", dftSchema, func)
          while (resultSet.next()) {
            val exprInfo = FunctionRegistry.expressions(func)._1
            assert(resultSet.getString(FUNCTION_CAT).isEmpty)
            assert(resultSet.getString(FUNCTION_SCHEM) === null)
            assert(resultSet.getString(FUNCTION_NAME) === exprInfo.getName)
            assert(resultSet.getString(REMARKS) ===
              s"Usage: ${exprInfo.getUsage}\nExtended Usage:${exprInfo.getExtended}")
            assert(resultSet.getString(SPECIFIC_NAME) === exprInfo.getClassName)
          }
        }
      }
    }
  }

  test("execute statement -  select decimal") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1.2BD as col1, 1.23BD AS col2")
      assert(resultSet.next())
      assert(resultSet.getBigDecimal("col1") === Decimal(1.2).toJavaBigDecimal)
      assert(resultSet.getBigDecimal("col2") === Decimal(1.23).toJavaBigDecimal)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DECIMAL)
      assert(metaData.getColumnType(2) === java.sql.Types.DECIMAL)
      assert(metaData.getPrecision(1) == 2)
      assert(metaData.getPrecision(2) == 3)
      assert(metaData.getScale(1) == 1)
      assert(metaData.getScale(2) == 2)
    }
  }

  test("Hive JDBC Database MetaData API Auditing") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq(
        () => metaData.allProceduresAreCallable(),
        () => metaData.getURL,
        () => metaData.getUserName,
        () => metaData.isReadOnly,
        () => metaData.nullsAreSortedHigh,
        () => metaData.nullsAreSortedLow,
        () => metaData.nullsAreSortedAtStart(),
        () => metaData.nullsAreSortedAtEnd(),
        () => metaData.usesLocalFiles(),
        () => metaData.usesLocalFilePerTable(),
        () => metaData.supportsMixedCaseIdentifiers(),
        () => metaData.supportsMixedCaseQuotedIdentifiers(),
        () => metaData.storesUpperCaseIdentifiers(),
        () => metaData.storesUpperCaseQuotedIdentifiers(),
        () => metaData.storesLowerCaseIdentifiers(),
        () => metaData.storesLowerCaseQuotedIdentifiers(),
        () => metaData.storesMixedCaseIdentifiers(),
        () => metaData.storesMixedCaseQuotedIdentifiers(),
        () => metaData.getSQLKeywords,
        () => metaData.nullPlusNonNullIsNull,
        () => metaData.supportsConvert,
        () => metaData.supportsTableCorrelationNames,
        () => metaData.supportsDifferentTableCorrelationNames,
        () => metaData.supportsExpressionsInOrderBy(),
        () => metaData.supportsOrderByUnrelated,
        () => metaData.supportsGroupByUnrelated,
        () => metaData.supportsGroupByBeyondSelect,
        () => metaData.supportsLikeEscapeClause,
        () => metaData.supportsMultipleTransactions,
        () => metaData.supportsMinimumSQLGrammar,
        () => metaData.supportsCoreSQLGrammar,
        () => metaData.supportsExtendedSQLGrammar,
        () => metaData.supportsANSI92EntryLevelSQL,
        () => metaData.supportsANSI92IntermediateSQL,
        () => metaData.supportsANSI92FullSQL,
        () => metaData.supportsIntegrityEnhancementFacility,
        () => metaData.isCatalogAtStart,
        () => metaData.supportsSubqueriesInComparisons,
        () => metaData.supportsSubqueriesInExists,
        () => metaData.supportsSubqueriesInIns,
        () => metaData.supportsSubqueriesInQuantifieds,
        // Spark support this, see https://issues.apache.org/jira/browse/SPARK-18455
        () => metaData.supportsCorrelatedSubqueries,
        () => metaData.supportsOpenCursorsAcrossCommit,
        () => metaData.supportsOpenCursorsAcrossRollback,
        () => metaData.supportsOpenStatementsAcrossCommit,
        () => metaData.supportsOpenStatementsAcrossRollback,
        () => metaData.getMaxBinaryLiteralLength,
        () => metaData.getMaxCharLiteralLength,
        () => metaData.getMaxColumnsInGroupBy,
        () => metaData.getMaxColumnsInIndex,
        () => metaData.getMaxColumnsInOrderBy,
        () => metaData.getMaxColumnsInSelect,
        () => metaData.getMaxColumnsInTable,
        () => metaData.getMaxConnections,
        () => metaData.getMaxCursorNameLength,
        () => metaData.getMaxIndexLength,
        () => metaData.getMaxSchemaNameLength,
        () => metaData.getMaxProcedureNameLength,
        () => metaData.getMaxCatalogNameLength,
        () => metaData.getMaxRowSize,
        () => metaData.doesMaxRowSizeIncludeBlobs,
        () => metaData.getMaxStatementLength,
        () => metaData.getMaxStatements,
        () => metaData.getMaxTableNameLength,
        () => metaData.getMaxTablesInSelect,
        () => metaData.getMaxUserNameLength,
        () => metaData.supportsTransactionIsolationLevel(1),
        () => metaData.supportsDataDefinitionAndDataManipulationTransactions,
        () => metaData.supportsDataManipulationTransactionsOnly,
        () => metaData.dataDefinitionCausesTransactionCommit,
        () => metaData.dataDefinitionIgnoredInTransactions,
        () => metaData.getColumnPrivileges("", "%", "%", "%"),
        () => metaData.getTablePrivileges("", "%", "%"),
        () => metaData.getBestRowIdentifier("", "%", "%", 0, true),
        () => metaData.getVersionColumns("", "%", "%"),
        () => metaData.getExportedKeys("", "default", ""),
        () => metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, 2),
        () => metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY),
        () => metaData.supportsNamedParameters(),
        () => metaData.supportsMultipleOpenResults,
        () => metaData.supportsGetGeneratedKeys,
        () => metaData.getSuperTypes("", "%", "%"),
        () => metaData.getSuperTables("", "%", "%"),
        () => metaData.getAttributes("", "%", "%", "%"),
        () => metaData.getResultSetHoldability,
        () => metaData.locatorsUpdateCopy,
        () => metaData.supportsStatementPooling,
        () => metaData.getRowIdLifetime,
        () => metaData.supportsStoredFunctionsUsingCallSyntax,
        () => metaData.autoCommitFailureClosesAllResultSets,
        () => metaData.getClientInfoProperties,
        () => metaData.getFunctionColumns("", "%", "%", "%"),
        () => metaData.getPseudoColumns("", "%", "%", "%"),
        () => metaData.generatedKeyAlwaysReturned).foreach { func =>
        val e = intercept[SQLFeatureNotSupportedException](func())
        assert(e.getMessage === "Method not supported")
      }

      import org.apache.kyuubi.KYUUBI_VERSION
      assert(metaData.allTablesAreSelectable)
      assert(metaData.getDatabaseProductName === "Spark SQL")
      assert(metaData.getDatabaseProductVersion === KYUUBI_VERSION)
      assert(metaData.getDriverName === "Hive JDBC")
      assert(metaData.getDriverVersion === HiveVersionInfo.getVersion)
      assert(metaData.getDatabaseMajorVersion === Utils.majorVersion(KYUUBI_VERSION))
      assert(metaData.getDatabaseMinorVersion === Utils.minorVersion(KYUUBI_VERSION))
      assert(metaData.getIdentifierQuoteString === " ",
        "This method returns a space \" \" if identifier quoting is not supported")
      assert(metaData.getNumericFunctions === "")
      assert(metaData.getStringFunctions === "")
      assert(metaData.getSystemFunctions === "")
      assert(metaData.getTimeDateFunctions === "")
      assert(metaData.getSearchStringEscape === "\\")
      assert(metaData.getExtraNameCharacters === "")
      assert(metaData.supportsAlterTableWithAddColumn())
      assert(!metaData.supportsAlterTableWithDropColumn())
      assert(metaData.supportsColumnAliasing())
      assert(metaData.supportsGroupBy)
      assert(!metaData.supportsMultipleResultSets)
      assert(!metaData.supportsNonNullableColumns)
      assert(metaData.supportsOuterJoins)
      assert(metaData.supportsFullOuterJoins)
      assert(metaData.supportsLimitedOuterJoins)
      assert(metaData.getSchemaTerm === "database")
      assert(metaData.getProcedureTerm === "UDF")
      assert(metaData.getCatalogTerm === "instance")
      assert(metaData.getCatalogSeparator === ".")
      assert(metaData.supportsSchemasInDataManipulation)
      assert(!metaData.supportsSchemasInProcedureCalls)
      assert(metaData.supportsSchemasInTableDefinitions)
      assert(!metaData.supportsSchemasInIndexDefinitions)
      assert(!metaData.supportsSchemasInPrivilegeDefinitions)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsCatalogsInDataManipulation)
      assert(!metaData.supportsCatalogsInProcedureCalls)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsCatalogsInTableDefinitions)
      assert(!metaData.supportsCatalogsInIndexDefinitions)
      assert(!metaData.supportsCatalogsInPrivilegeDefinitions)
      assert(!metaData.supportsPositionedDelete)
      assert(!metaData.supportsPositionedUpdate)
      assert(!metaData.supportsSelectForUpdate)
      assert(!metaData.supportsStoredProcedures)
      // This is actually supported, but hive jdbc package return false
      assert(!metaData.supportsUnion)
      assert(metaData.supportsUnionAll)
      assert(metaData.getMaxColumnNameLength === 128)
      assert(metaData.getDefaultTransactionIsolation === java.sql.Connection.TRANSACTION_NONE)
      assert(!metaData.supportsTransactions)
      assert(!metaData.getProcedureColumns("", "%", "%", "%").next())
      intercept[HiveSQLException](metaData.getPrimaryKeys("", "default", ""))
      assert(!metaData.getImportedKeys("", "default", "").next())
      intercept[HiveSQLException] {
        metaData.getCrossReference("", "default", "src", "", "default", "src2")
      }
      assert(!metaData.getIndexInfo("", "default", "src", true, true).next())

      assert(metaData.supportsResultSetType(new Random().nextInt()))
      assert(!metaData.supportsBatchUpdates)
      assert(!metaData.getUDTs(",", "%", "%", null).next())
      assert(!metaData.supportsSavepoints)
      assert(!metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT))
      assert(metaData.getJDBCMajorVersion === 3)
      assert(metaData.getJDBCMinorVersion === 0)
      assert(metaData.getSQLStateType === DatabaseMetaData.sqlStateSQL)
      assert(metaData.getMaxLogicalLobSize === 0)
      assert(!metaData.supportsRefCursors)
    }
  }

  test("get operation status") {
    val sql = "select date_sub(date'2011-11-11', '1')"

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
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle( tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("set -v")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(1)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      val logs = tFetchResultsResp.getResults.getColumns.get(0).getStringVal.getValues.asScala
      assert(logs.exists(_.contains(classOf[ExecuteStatement].getCanonicalName)))

      tFetchResultsReq.setFetchType(0)
      val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq)
      val rs = tFetchResultsResp1.getResults.getColumns.get(0).getStringVal.getValues.asScala
      assert(rs.contains("spark.sql.shuffle.partitions"))

      val tCloseSessionReq = new TCloseSessionReq()
      tCloseSessionReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      val tCloseSessionResp = client.CloseSession(tCloseSessionReq)
      assert(tCloseSessionResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("set session conf") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val conf = Map(
        "use:database" -> "default",
        "spark.sql.shuffle.partitions" -> "4",
        "set:hiveconf:spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "set:hivevar:spark.sql.adaptive.enabled" -> "true")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("set")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq)
      val columns = tFetchResultsResp1.getResults.getColumns
      val rs = columns.get(0).getStringVal.getValues.asScala.zip(
        columns.get(1).getStringVal.getValues.asScala)
      rs foreach {
        case ("spark.sql.shuffle.partitions", v) => assert(v === "4")
        case ("spark.sql.autoBroadcastJoinThreshold", v) => assert(v === "-1")
        case ("spark.sql.adaptive.enabled", v) => assert(v.toBoolean)
        case _ =>
      }
      assert(spark.conf.get("spark.sql.shuffle.partitions") === "200")

      val tCloseSessionReq = new TCloseSessionReq()
      tCloseSessionReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      val tCloseSessionResp = client.CloseSession(tCloseSessionReq)
      assert(tCloseSessionResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("set session conf - static and core") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val queue = "spark.yarn.queue"
      val conf = Map("use:database" -> "default",
        "spark.sql.globalTempDatabase" -> "temp",
        queue -> "new")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(spark.conf.get("spark.sql.globalTempDatabase") === "global_temp")
      assert(spark.conf.getOption(queue).isEmpty)
    }
  }

  test("set session conf - wrong database") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val conf = Map("use:database" -> "default2")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      assert(status.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(status.getErrorMessage.contains("Database 'default2' does not exist"))
    }
  }

  test("not allow to operate closed session or operation") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("set")
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
      assert(tFetchResultsResp.getStatus.getErrorMessage startsWith "Invalid OperationHandle" +
        " [type=EXECUTE_STATEMENT, identifier:")

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
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("set")
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
}
