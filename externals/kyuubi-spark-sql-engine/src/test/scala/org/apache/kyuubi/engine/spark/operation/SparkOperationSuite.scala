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

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.hadoop.hdfs.security.token.delegation.{DelegationTokenIdentifier => HDFSTokenIdent}
import org.apache.hadoop.hive.thrift.{DelegationTokenIdentifier => HiveTokenIdent}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.kyuubi.SparkContextHelper
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.types._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.schema.SchemaHelper.TIMESTAMP_NTZ
import org.apache.kyuubi.engine.spark.util.SparkCatalogUtils
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.operation.{HiveMetadataTests, SparkQueryTests}
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.KyuubiHadoopUtils
import org.apache.kyuubi.util.SemanticVersion

class SparkOperationSuite extends WithSparkSQLEngine with HiveMetadataTests with SparkQueryTests {

  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = Map.empty

  test("get table types") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = SparkCatalogUtils.sparkTableTypes.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("audit Spark engine MetaData") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      assert(metaData.getDatabaseProductName === "Spark SQL")
      assert(metaData.getDatabaseProductVersion === SPARK_VERSION)
      val ver = SemanticVersion(SPARK_VERSION)
      assert(metaData.getDatabaseMajorVersion === ver.majorVersion)
      assert(metaData.getDatabaseMinorVersion === ver.minorVersion)
    }
  }

  test("get columns operation") {
    val tableName = "spark_get_col_operation"
    var schema = new StructType()
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
      .add("c17", "struct<X: string>", nullable = true, "17")
      .add("c18", "interval day", nullable = true, "18")
      .add("c19", "interval year", nullable = true, "19")
    // since spark3.4.0
    if (SPARK_ENGINE_RUNTIME_VERSION >= "3.4") {
      schema = schema.add("c20", "timestamp_ntz", nullable = true, "20")
    }

    val ddl =
      s"""
         |CREATE TABLE IF NOT EXISTS $defaultSchema.$tableName (
         |  ${schema.toDDL}
         |)
         |USING parquet""".stripMargin

    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val metaData = statement.getConnection.getMetaData

      Seq("%", null, ".*", "c.*") foreach { columnPattern =>
        val rowSet = metaData.getColumns("", defaultSchema, tableName, columnPattern)

        import java.sql.Types._
        val expectedJavaTypes = Seq(
          BOOLEAN,
          TINYINT,
          SMALLINT,
          INTEGER,
          BIGINT,
          FLOAT,
          DOUBLE,
          DECIMAL,
          DECIMAL,
          VARCHAR,
          ARRAY,
          ARRAY,
          JAVA_OBJECT,
          DATE,
          TIMESTAMP,
          STRUCT,
          BINARY,
          STRUCT,
          OTHER,
          OTHER,
          TIMESTAMP)

        var pos = 0

        while (rowSet.next()) {
          assert(rowSet.getString(TABLE_CAT) === SparkCatalogUtils.SESSION_CATALOG)
          assert(rowSet.getString(TABLE_SCHEM) === defaultSchema)
          assert(rowSet.getString(TABLE_NAME) === tableName)
          assert(rowSet.getString(COLUMN_NAME) === schema(pos).name)
          assert(rowSet.getInt(DATA_TYPE) === expectedJavaTypes(pos))
          assert(rowSet.getString(TYPE_NAME) === schema(pos).dataType.sql)

          val colSize = rowSet.getInt(COLUMN_SIZE)
          schema(pos).dataType match {
            case StringType | BinaryType | _: ArrayType | _: MapType => assert(colSize === 0)
            case d: DecimalType => assert(colSize === d.precision)
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
            case ntz if ntz.getClass.getSimpleName.equals(TIMESTAMP_NTZ) =>
              assert(decimalDigits === 6)
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

        assert(pos === schema.length, "all columns should have been verified")
      }

      val rowSet = metaData.getColumns(null, "*", "not_exist", "not_exist")
      assert(!rowSet.next())
    }
  }

  test("get columns operation should handle interval column properly") {
    val viewName = "view_interval"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName AS SELECT INTERVAL 1 DAY AS i"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, null)
      while (rowSet.next()) {
        assert(rowSet.getString(TABLE_CAT) === SparkCatalogUtils.SESSION_CATALOG)
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
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName AS SELECT NULL AS n"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, "n")
      while (rowSet.next()) {
        assert(rowSet.getString(TABLE_CAT) === SparkCatalogUtils.SESSION_CATALOG)
        assert(rowSet.getString(TABLE_SCHEM) === "global_temp")
        assert(rowSet.getString(TABLE_NAME) === viewName)
        assert(rowSet.getString(COLUMN_NAME) === "n")
        assert(rowSet.getInt(DATA_TYPE) === java.sql.Types.NULL)
        assert(rowSet.getString(TYPE_NAME).equalsIgnoreCase(NullType.sql))
        assert(rowSet.getInt(COLUMN_SIZE) === 1)
        assert(rowSet.getInt(DECIMAL_DIGITS) === 0)
        assert(rowSet.getInt(NUM_PREC_RADIX) === 0)
        assert(rowSet.getInt(NULLABLE) === 1)
        assert(rowSet.getString(REMARKS) === "")
        assert(rowSet.getInt(ORDINAL_POSITION) === 0)
        assert(rowSet.getString(IS_NULLABLE) === "YES")
        assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
      }
    }
  }

  test("get functions") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val apis = Seq(metaData.getFunctions _, metaData.getProcedures _)
      Seq("to_timestamp", "date_part", "lpad", "date_format", "cos", "sin").foreach { func =>
        apis.foreach { apiFunc =>
          val resultSet = apiFunc("", defaultSchema, func)
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

  test("execute statement - select decimal") {
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

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString("tmp.hello") === "tmp.hello")
    }
  }

  test("test fetch orientation") {
    val sql = "SELECT id FROM range(2)"

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
      val idSeq1 = tFetchResultsResp1.getResults.getColumns.get(0).getI64Val.getValues.asScala
      assertResult(Seq(0L))(idSeq1)

      // fetch next from first row
      val tFetchResultsReq2 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1)
      val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
      assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val idSeq2 = tFetchResultsResp2.getResults.getColumns.get(0).getI64Val.getValues.asScala
      assertResult(Seq(1L))(idSeq2)

      // fetch prior from second row, expected got first row
      val tFetchResultsReq3 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_PRIOR, 1)
      val tFetchResultsResp3 = client.FetchResults(tFetchResultsReq3)
      assert(tFetchResultsResp3.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val idSeq3 = tFetchResultsResp3.getResults.getColumns.get(0).getI64Val.getValues.asScala
      assertResult(Seq(0L))(idSeq3)

      // fetch first
      val tFetchResultsReq4 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_FIRST, 3)
      val tFetchResultsResp4 = client.FetchResults(tFetchResultsReq4)
      assert(tFetchResultsResp4.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val idSeq4 = tFetchResultsResp4.getResults.getColumns.get(0).getI64Val.getValues.asScala
      assertResult(Seq(0L, 1L))(idSeq4)
    }
  }

  test("test fetch orientation with incremental collect mode") {
    val sql = "SELECT id FROM range(2)"

    withSessionConf(Map(KyuubiConf.ENGINE_SPARK_OPERATION_INCREMENTAL_COLLECT.key -> "true"))()() {
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
        val idSeq1 = tFetchResultsResp1.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq(0L))(idSeq1)

        // fetch next from first row
        val tFetchResultsReq2 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1)
        val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
        assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq2 = tFetchResultsResp2.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq(1L))(idSeq2)

        // fetch prior from second row, expected got first row
        val tFetchResultsReq3 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_PRIOR, 1)
        val tFetchResultsResp3 = client.FetchResults(tFetchResultsReq3)
        assert(tFetchResultsResp3.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq3 = tFetchResultsResp3.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq(0L))(idSeq3)

        // fetch first
        val tFetchResultsReq4 = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_FIRST, 3)
        val tFetchResultsResp4 = client.FetchResults(tFetchResultsReq4)
        assert(tFetchResultsResp4.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq4 = tFetchResultsResp4.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq(0L, 1L))(idSeq4)
      }
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
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setRunAsync(true)
      tExecuteStatementReq.setStatement("set -v")
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
      tExecuteStatementReq.setRunAsync(true)
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val operationHandle = tExecuteStatementResp.getOperationHandle
      waitForOperationToComplete(client, operationHandle)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
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
      val conf = Map(
        "use:database" -> "default",
        "spark.sql.globalTempDatabase" -> "temp",
        queue -> "new",
        s"set:hiveconf:$queue" -> "newnew",
        s"set:hivevar:$queue" -> "newnewnew")
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
      val dbName = "default2"
      val conf = Map("use:database" -> dbName)
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      val errorMessage = status.getErrorMessage
      assert(status.getStatusCode === TStatusCode.ERROR_STATUS)
      if (SPARK_ENGINE_RUNTIME_VERSION >= "3.4") {
        assert(errorMessage.contains("[SCHEMA_NOT_FOUND]"))
      } else {
        assert(errorMessage.contains(s"Database '$dbName' not found"))
      }
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

  test("env:* variables can not be set") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("chengpan")
      req.setPassword("123")
      val conf = Map(
        "set:env:ABC" -> "xyz")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      assert(status.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(status.getErrorMessage contains s"env:* variables can not be set")
    }
  }

  test("test variable substitution") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("chengpan")
      req.setPassword("123")
      val conf = Map(
        "use:database" -> "default",
        "set:hiveconf:a" -> "x",
        "set:hivevar:b" -> "y",
        "set:metaconf:c" -> "z",
        "set:system:s" -> "s")
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)

      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      // hive matched behaviors
      tExecuteStatementReq.setStatement(
        """
          |select
          | '${hiveconf:a}' as col_0,
          | '${hivevar:b}'  as col_1,
          | '${b}'          as col_2
          |""".stripMargin)
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(tFetchResultsResp.getResults.getColumns.get(0).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp.getResults.getColumns.get(1).getStringVal.getValues.get(0) === "y")
      assert(tFetchResultsResp.getResults.getColumns.get(2).getStringVal.getValues.get(0) === "y")

      val tExecuteStatementReq2 = new TExecuteStatementReq()
      tExecuteStatementReq2.setSessionHandle(tOpenSessionResp.getSessionHandle)
      // spark specific behaviors
      tExecuteStatementReq2.setStatement(
        """
          |select
          | '${a}'             as col_0,
          | '${hivevar:a}'     as col_1,
          | '${spark:a}'       as col_2,
          | '${sparkconf:a}'   as col_3,
          | '${not_exist_var}' as col_4,
          | '${c}'             as col_5,
          | '${s}'             as col_6
          |""".stripMargin)
      val tExecuteStatementResp2 = client.ExecuteStatement(tExecuteStatementReq2)
      val tFetchResultsReq2 = new TFetchResultsReq()
      tFetchResultsReq2.setOperationHandle(tExecuteStatementResp2.getOperationHandle)
      tFetchResultsReq2.setFetchType(0)
      tFetchResultsReq2.setMaxRows(1)
      val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
      assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(tFetchResultsResp2.getResults.getColumns.get(0).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(1).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(2).getStringVal.getValues.get(0) === "x")
      assert(tFetchResultsResp2.getResults.getColumns.get(3).getStringVal.getValues.get(0) === "x")
      // for not exist vars, hive return "${not_exist_var}" itself, but spark return ""
      assert(tFetchResultsResp2.getResults.getColumns.get(4).getStringVal.getValues.get(0) === "")

      assert(tFetchResultsResp2.getResults.getColumns.get(5).getStringVal.getValues.get(0) === "z")
      assert(tFetchResultsResp2.getResults.getColumns.get(6).getStringVal.getValues.get(0) === "s")
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

  test("send credentials by TRenewDelegationTokenReq") {
    // Simulate a secured SparkSQLEngine's credentials
    val currentTime = System.currentTimeMillis()
    val hdfsTokenAlias = new Text("HDFS1")
    val hiveTokenAlias = new Text("hive.server2.delegation.token")
    val creds1 = createCredentials(currentTime, hdfsTokenAlias.toString, hiveTokenAlias.toString)
    // SparkSQLEngine may have token alias unknown to Kyuubi Server
    val unknownTokenAlias = new Text("UNKNOWN")
    val unknownToken = newHDFSToken(currentTime)
    creds1.addToken(unknownTokenAlias, unknownToken)
    SparkContextHelper.updateDelegationTokens(spark.sparkContext, creds1)

    val metastoreUris = "thrift://localhost:9083,thrift://localhost:9084"

    whenMetaStoreURIsSetTo(metastoreUris) { uris =>
      withThriftClient { client =>
        val req = new TOpenSessionReq()
        req.setUsername("username")
        req.setPassword("password")
        val tOpenSessionResp = client.OpenSession(req)

        def sendCredentials(client: TCLIService.Iface, credentials: Credentials): Unit = {
          val renewDelegationTokenReq = new TRenewDelegationTokenReq()
          renewDelegationTokenReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
          renewDelegationTokenReq.setDelegationToken(
            KyuubiHadoopUtils.encodeCredentials(credentials))
          val renewDelegationTokenResp = client.RenewDelegationToken(renewDelegationTokenReq)
          assert(renewDelegationTokenResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        }

        // Send new credentials
        val creds2 = createCredentials(currentTime + 1, hdfsTokenAlias.toString, uris)
        // Kyuubi Server may have extra HDFS and Hive delegation tokens
        val extraHDFSToken = newHDFSToken(currentTime + 1)
        creds2.addToken(new Text("HDFS2"), extraHDFSToken)
        sendCredentials(client, creds2)
        // SparkSQLEngine's tokens should be updated
        var engineCredentials = UserGroupInformation.getCurrentUser.getCredentials
        assert(engineCredentials.getToken(hdfsTokenAlias) == creds2.getToken(hdfsTokenAlias))
        assert(
          engineCredentials.getToken(hiveTokenAlias) == creds2.getToken(new Text(metastoreUris)))
        // Unknown tokens should not be updated
        assert(engineCredentials.getToken(unknownTokenAlias) == unknownToken)

        // Send old credentials
        val creds3 = createCredentials(currentTime, hdfsTokenAlias.toString, metastoreUris)
        sendCredentials(client, creds3)
        // SparkSQLEngine's tokens should not be updated
        engineCredentials = UserGroupInformation.getCurrentUser.getCredentials
        assert(engineCredentials.getToken(hdfsTokenAlias) == creds2.getToken(hdfsTokenAlias))
        assert(
          engineCredentials.getToken(hiveTokenAlias) == creds2.getToken(new Text(metastoreUris)))

        // No matching tokens
        val creds4 = createCredentials(currentTime + 2, "HDFS2", "thrift://localhost:9085")
        sendCredentials(client, creds4)
        // No token is updated
        engineCredentials = UserGroupInformation.getCurrentUser.getCredentials
        assert(engineCredentials.getToken(hdfsTokenAlias) == creds2.getToken(hdfsTokenAlias))
        assert(
          engineCredentials.getToken(hiveTokenAlias) == creds2.getToken(new Text(metastoreUris)))
      }
    }
  }

  test("KYUUBI #5030: Support get query id in Spark engine") {
    withJdbcStatement() { stmt =>
      stmt.executeQuery("SELECT 1")
      val queryId = stmt.asInstanceOf[KyuubiStatement].getQueryId
      assert(queryId != null && queryId.nonEmpty)
    }
  }

  private def whenMetaStoreURIsSetTo(uris: String)(func: String => Unit): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val origin = conf.get("hive.metastore.uris", "")
    conf.set("hive.metastore.uris", uris)
    try func.apply(uris)
    finally {
      conf.set("hive.metastore.uris", origin)
    }
  }

  private def createCredentials(
      issueDate: Long,
      hdfsTokenAlias: String,
      hiveTokenAlias: String): Credentials = {
    val credentials = new Credentials()
    credentials.addToken(new Text(hdfsTokenAlias), newHDFSToken(issueDate))
    credentials.addToken(new Text(hiveTokenAlias), newHiveToken(issueDate))
    credentials
  }

  private def newHDFSToken(issueDate: Long): Token[TokenIdentifier] = {
    val who = new Text("who")
    val tokenId = new HDFSTokenIdent(who, who, who)
    tokenId.setIssueDate(issueDate)
    newToken(tokenId)
  }

  private def newHiveToken(issueDate: Long): Token[TokenIdentifier] = {
    val who = new Text("who")
    val tokenId = new HiveTokenIdent(who, who, who)
    tokenId.setIssueDate(issueDate)
    newToken(tokenId)
  }

  private def newToken(tokeIdent: TokenIdentifier): Token[TokenIdentifier] = {
    val token = new Token[TokenIdentifier]
    token.setID(tokeIdent.getBytes)
    token.setKind(tokeIdent.getKind)
    val bytes = new Array[Byte](128)
    Random.nextBytes(bytes)
    token.setPassword(bytes)
    token
  }
}
