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

package org.apache.kyuubi.operation

import java.sql.{SQLException, SQLTimeoutException}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TGetResultSetMetadataReq, TOpenSessionReq, TStatusCode}

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.util.SparkVersionUtil.isSparkVersionAtLeast

trait SparkQueryTests extends SparkDataTypeTests with HiveJDBCTestHelper {

  protected lazy val httpMode = false

  test("execute statement - analysis exception") {
    val sql = "select date_sub(date'2011-11-11', '1.2')"
    val errors = Set(
      "The second argument of 'date_sub' function needs to be an integer.",
      // unquoted since Spark-3.4, see https://github.com/apache/spark/pull/36693
      "The second argument of date_sub function needs to be an integer.")

    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery(sql)
      }
      assert(errors.exists(msg => e.getMessage.contains(msg)))
    }
  }

  test("execute statement - select with variable substitution") {
    assume(!httpMode)

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

  test("execute statement - select with builtin functions") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT substring('kentyao', 1)")
      assert(resultSet.next())
      assert(resultSet.getString("substring(kentyao, 1, 2147483647)") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("query time out shall respect client-side if no server-side control") {
    withJdbcStatement() { statement =>
      statement.setQueryTimeout(1)
      val e = intercept[SQLTimeoutException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e.contains("Query timed out after"))

      statement.setQueryTimeout(0)
      val rs1 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs1.next()
      assert(rs1.getString(1) == "test")

      statement.setQueryTimeout(-1)
      val rs2 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs2.next()
      assert(rs2.getString(1) == "test")
    }
  }

  test("kyuubi defined function - kyuubi_version") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT kyuubi_version()")
      assert(rs.next())
      assert(rs.getString(1) == KYUUBI_VERSION)
    }
  }

  test("kyuubi defined function - engine_name") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT engine_name()")
      assert(rs.next())
      assert(StringUtils.isNotBlank(rs.getString(1)))
    }
  }

  test("kyuubi defined function - engine_id") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT engine_id()")
      assert(rs.next())
      assert(StringUtils.isNotBlank(rs.getString(1)))
    }
  }

  test("KYUUBI #1059: Plan only operations") {
    val tableName = "t"
    val ddl = s"create table $tableName(a int) using parquet"
    val dql = s"select * from $tableName"
    val setkey = "SET kyuubi.operation.plan.only.mode"
    withJdbcStatement("t") { statement =>
      try {
        val assertTableOrViewNotfound: (Exception, String) => Unit = (e, tableName) => {
          if (isSparkVersionAtLeast("3.4")) {
            assert(e.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND]"))
            assert(e.getMessage.contains(s"The table or view `$tableName` cannot be found."))
          } else {
            assert(e.getMessage.contains("Table or view not found"))
          }
        }
        statement.execute("SET kyuubi.operation.plan.only.mode=optimize")
        val set = statement.executeQuery(ddl)
        assert(set.next())
        assert(set.getString("plan") startsWith "Create")
        val set0 = statement.executeQuery(setkey)
        assert(set0.next())
        assert(set0.getString(2) === "optimize")
        val e1 = intercept[SQLException](statement.executeQuery(dql))
        assertTableOrViewNotfound(e1, tableName)
        statement.execute("SET kyuubi.operation.plan.only.mode=analyze")
        val e2 = intercept[SQLException](statement.executeQuery(dql))
        assertTableOrViewNotfound(e2, tableName)
        statement.execute("SET kyuubi.operation.plan.only.mode=parse")
        val set1 = statement.executeQuery(dql)
        assert(set1.next())
        assert(set1.getString("plan") contains "Unresolved")
      } finally {
        statement.executeQuery("SET kyuubi.operation.plan.only.mode=none")
        statement.executeQuery(ddl)
        val res = statement.executeQuery(dql)
        assert(!res.next(), "table t exists in none mode")
      }
    }
  }

  test("execute simple scala code") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val rs = statement.executeQuery("spark.version")
      rs.next()
      // scala repl will return resX = YYYYY, and here we only check YYYYY
      val sparkVer = rs.getString(1).split("=")(1).trim
      assert("\\d\\.\\d\\.\\d(-SNAPSHOT)?".r.pattern.matcher(sparkVer).matches())
      assert(rs.getMetaData.getColumnName(1) === "output")
    }
  }

  test("execute simple scala code with result returned") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val code =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .toDF
          |""".stripMargin
      val rs1 = statement.executeQuery(code)
      rs1.next()
      assert(rs1.getString(1) startsWith "df: org.apache.spark.sql.DataFrame")

      // continue
      val rs2 = statement.executeQuery("df.count()")
      rs2.next()
      assert(rs2.getString(1).endsWith("5"))

      // continue
      val rs3 = statement.executeQuery("result.set(df)")
      for (i <- Range(0, 10, 2)) {
        assert(rs3.next)
        assert(rs3.getInt(1) === i)
      }

      // switch to sql
      val set =
        """
          |spark.conf.set("kyuubi.operation.language", "SQL")
          |""".stripMargin
      val t = statement.executeQuery(set)
      t.next()
      val rs4 = statement.executeQuery("select 12345")
      assert(rs4.next())
      assert(rs4.getInt(1) === 12345)

      // switch to scala again
      statement.execute("SET kyuubi.operation.language=scala")
      val code2 =
        """
          |/* this
          | * is
          | * a
          | * multi-line comments
          | */
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map(x => (x, x + 1, x * 2)) // this is a single-line comment
          |  .toDF
          |""".stripMargin
      val rs5 = statement.executeQuery(code2)
      rs5.next()
      assert(rs5.getString(1) startsWith "df: org.apache.spark.sql.DataFrame")

      // re-assign
      val rs6 = statement.executeQuery("result.set(df)")
      for (i <- Range(0, 10, 2)) {
        assert(rs6.next)
        assert(rs6.getInt(2) === i + 1)
      }
    }
  }

  test("incomplete scala code block will fail") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      // incomplete code block
      val incompleteCode =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map {
          |    x => (x, x + 1, x * 2)
          |""".stripMargin
      val e = intercept[SQLException](statement.executeQuery(incompleteCode))
      assert(e.getMessage contains "Incomplete code:")
    }
  }

  test("scala code compile error will fail") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      // incomplete code block
      val incompleteCode =
        """
          |val df = spark
          |  .range(0, 10, 2, 1)
          |  .map { x => (x, x + 1, y * 2) } // y is missing
          |""".stripMargin
      val e = intercept[SQLException](statement.executeQuery(incompleteCode))
      assert(e.getMessage contains "not found: value y")
    }
  }

  test("scala code with console output") {
    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val code = """spark.sql("SET kyuubi.operation.language").show(false)"""
      val rs = statement.executeQuery(code)

      var foundOperationLangItem = false
      while (rs.next() && !foundOperationLangItem) {
        foundOperationLangItem = rs.getString(1).contains("kyuubi.operation.language")
      }
      assert(foundOperationLangItem)
    }
  }

  test("scala code with loading external package at runtime ") {
    val jarDir = Utils.createTempDir().toFile

    withJdbcStatement() { statement =>
      statement.execute("SET kyuubi.operation.language=scala")
      val udfCode =
        """
          |package test.utils
          |
          |object Math {
          |def add(x: Int, y: Int): Int = x + y
          |}
          |
          |""".stripMargin

      val jarFile = UserJarTestUtils.createJarFile(
        udfCode,
        "test",
        s"test-function-${UUID.randomUUID}.jar",
        jarDir.toString)
      val jarBytes = FileUtils.readFileToByteArray(jarFile)
      val jarStr = new String(java.util.Base64.getEncoder().encode(jarBytes))
      val jarName = s"test-function-${UUID.randomUUID}.jar"

      val code0 = """spark.sql("SET kyuubi.operation.language").show(false)"""
      statement.execute(code0)

      // Generate a jar package in spark engine
      val batchCode =
        s"""
           |import java.io.{BufferedOutputStream, File, FileOutputStream}
           |val dir = spark.sparkContext.getConf.get("spark.repl.class.outputDir")
           |val jarFile = new File(dir, "$jarName")
           |val bos = new BufferedOutputStream(new FileOutputStream(jarFile))
           |val path = "$jarStr"
           |bos.write(java.util.Base64.getDecoder.decode(path))
           |bos.close()
           |val jarPath = jarFile.getAbsolutePath
           |val fileSize = jarFile.length
           |""".stripMargin
      batchCode.split("\n").filter(_.nonEmpty).foreach { code =>
        val rs = statement.executeQuery(code)
        rs.next()
        // scalastyle:off
        println(rs.getString(1))
      // scalastyle:on
      }

      val code1 = s"""spark.sql("add jar " + jarPath)"""
      val code2 = """val x = test.utils.Math.add(1,2)"""
      statement.execute(code1)
      val rs = statement.executeQuery(code2)
      rs.next()
      assert(rs.getString(1) == "x: Int = 3")
    }
  }

  test("kyuubi #3311: Operation language with an incorrect value") {
    withSessionConf()(Map(KyuubiConf.OPERATION_LANGUAGE.key -> "SQL"))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_LANGUAGE.key}=AAA")
        val e = intercept[SQLException](statement.executeQuery("select 1"))
        assert(e.getMessage.contains("The operation language UNKNOWN doesn't support"))
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_LANGUAGE.key}=SQL")
        val result = statement.executeQuery("select 1")
        assert(result.next())
        assert(result.getInt(1) === 1)
      }
    }
  }

  test("operation metadata hint - __kyuubi_operation_result_codec__") {
    assume(!httpMode)
    withSessionHandle { (client, handle) =>
      def checkStatusAndResultSetCodecHint(
          sql: String,
          expectedCodec: String): Unit = {
        val stmtReq = new TExecuteStatementReq()
        stmtReq.setSessionHandle(handle)
        stmtReq.setStatement(sql)
        val tExecuteStatementResp = client.ExecuteStatement(stmtReq)
        val opHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, opHandle)
        val metaReq = new TGetResultSetMetadataReq(opHandle)
        val resp = client.GetResultSetMetadata(metaReq)
        assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        val expectedResultSetCodecHint = s"__kyuubi_operation_result_codec__=$expectedCodec"
        assert(resp.getStatus.getInfoMessages.contains(expectedResultSetCodecHint))
      }
      checkStatusAndResultSetCodecHint(
        sql = "SELECT 1",
        expectedCodec = "simple")
      checkStatusAndResultSetCodecHint(
        sql = "set kyuubi.operation.result.codec=arrow",
        expectedCodec = "arrow")
      checkStatusAndResultSetCodecHint(
        sql = "SELECT 1",
        expectedCodec = "arrow")
      checkStatusAndResultSetCodecHint(
        sql = "set kyuubi.operation.result.codec=simple",
        expectedCodec = "simple")
      checkStatusAndResultSetCodecHint(
        sql = "set kyuubi.operation.result.codec",
        expectedCodec = "simple")
    }
  }
}
