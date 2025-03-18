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

package org.apache.kyuubi.engine.spark

import java.io.PrintWriter
import java.nio.file.Files
import java.sql.SQLTimeoutException
import java.util.Properties

import scala.sys.process._

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.jdbc.hive.{KyuubiSQLException, KyuubiStatement}
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.tags.PySparkTest

@PySparkTest
class PySparkTests extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = new KyuubiConf

  test("pyspark support") {
    val code = "print(1)"
    val output = "1"
    runPySparkTest(code, output)
  }

  test("pyspark support - multi-line") {
    val code =
      """
        |for i in [1, 2, 3]:
        |    print(i)
        |""".stripMargin
    val output = "1\n2\n3"
    runPySparkTest(code, output)
  }

  test("pyspark support - call spark.sql") {
    val code =
      """
        |spark.sql("select 1").show()
        |""".stripMargin
    val output =
      """|+---+
         ||  1|
         |+---+
         ||  1|
         |+---+""".stripMargin
    runPySparkTest(code, output)
  }

  test("executePython support timeout") {
    val driver = new KyuubiHiveDriver()
    val connection = driver.connect(getJdbcUrl, new Properties())
    val statement = connection.createStatement().asInstanceOf[KyuubiStatement]
    statement.setQueryTimeout(5)
    try {
      var code = "spark.sql(\"select java_method('java.lang.Thread', 'sleep', 10000L)\").show()"
      var e = intercept[SQLTimeoutException] {
        statement.executePython(code)
      }.getMessage
      assert(e.contains("Query timed out"))
      code = "bad_code"
      e = intercept[KyuubiSQLException](statement.executePython(code)).getMessage
      assert(e.contains("Interpret error"))
    } finally {
      statement.close()
      connection.close()
    }
  }

  test("binding python/sql spark session") {
    checkPythonRuntimeAndVersion()
    withMultipleConnectionJdbcStatement()({ statement =>
      statement.executeQuery("SET kyuubi.operation.language=PYTHON")

      // set hello=kyuubi in python
      val set1 =
        """
          |spark.sql("set hello=kyuubi").show()
          |""".stripMargin
      val output1 =
        """|+-----+------+
           ||  key| value|
           |+-----+------+
           ||hello|kyuubi|
           |+-----+------+""".stripMargin
      val resultSet1 = statement.executeQuery(set1)
      assert(resultSet1.next())
      assert(resultSet1.getString("status") === "ok")
      assert(resultSet1.getString("output") === output1)

      val set2 =
        """
          |spark.sql("SET kyuubi.operation.language=SQL").show(truncate = False)
          |""".stripMargin
      val output2 =
        """|+-------------------------+-----+
           ||key                      |value|
           |+-------------------------+-----+
           ||kyuubi.operation.language|SQL  |
           |+-------------------------+-----+""".stripMargin
      val resultSet2 = statement.executeQuery(set2)
      assert(resultSet2.next())
      assert(resultSet2.getString("status") === "ok")
      assert(resultSet2.getString("output") === output2)

      // get hello value in sql
      val resultSet3 = statement.executeQuery("set hello")
      assert(resultSet3.next())
      assert(resultSet3.getString("key") === "hello")
      assert(resultSet3.getString("value") === "kyuubi")
    })
  }

  test("Support python magic syntax for python notebook") {
    checkPythonRuntimeAndVersion()
    withSessionConf()(Map(KyuubiConf.ENGINE_SPARK_PYTHON_MAGIC_ENABLED.key -> "true"))() {
      withMultipleConnectionJdbcStatement()({ stmt =>
        val statement = stmt.asInstanceOf[KyuubiStatement]
        statement.executePython("x = [[1, 'a', {'k1':'v1'}], [3, 'b', {'k2':'v2'}]]")

        val resultSet1 = statement.executePython("%json x")
        assert(resultSet1.next())
        val output1 = resultSet1.getString("output")
        assert(output1 == """{"application/json":[[1,"a",{"k1":"v1"}],[3,"b",{"k2":"v2"}]]}""")

        val resultSet2 = statement.executePython("%table x")
        assert(resultSet2.next())
        val output2 = resultSet2.getString("output")
        assert(output2 == "{\"application/vnd.livy.table.v1+json\":{" +
          "\"headers\":[" +
          "{\"name\":\"0\",\"type\":\"INT_TYPE\"}," +
          "{\"name\":\"1\",\"type\":\"STRING_TYPE\"}," +
          "{\"name\":\"2\",\"type\":\"MAP_TYPE\"}" +
          "]," +
          "\"data\":[" +
          "[1,\"a\",{\"k1\":\"v1\"}]," +
          "[3,\"b\",{\"k2\":\"v2\"}]" +
          "]}}")

        Seq("table", "json", "matplot").foreach { magic =>
          val e = intercept[KyuubiSQLException] {
            statement.executePython(s"%$magic invalid_value")
          }.getMessage
          assert(e.contains("KeyError: 'invalid_value'"))
        }

        statement.executePython("y = [[1, 2], [3, 'b']]")
        var e = intercept[KyuubiSQLException] {
          statement.executePython("%table y")
        }.getMessage
        assert(e.contains("table rows have different types"))

        e = intercept[KyuubiSQLException] {
          statement.executePython("%magic_unknown")
        }.getMessage
        assert(e.contains("unknown magic command 'magic_unknown'"))
      })
    }

    withSessionConf()(Map(KyuubiConf.ENGINE_SPARK_PYTHON_MAGIC_ENABLED.key -> "false"))() {
      withMultipleConnectionJdbcStatement()({ stmt =>
        val statement = stmt.asInstanceOf[KyuubiStatement]
        statement.executePython("x = [[1, 'a'], [3, 'b']]")
        val e = intercept[KyuubiSQLException] {
          statement.executePython("%json x")
        }.getMessage
        assert(e.contains("SyntaxError: invalid syntax"))
      })
    }
  }

  test("catch all exception when dump the result to json") {
    checkPythonRuntimeAndVersion()
    withSessionConf()(Map(KyuubiConf.ENGINE_SPARK_PYTHON_MAGIC_ENABLED.key -> "true"))() {
      withMultipleConnectionJdbcStatement()({ stmt =>
        val statement = stmt.asInstanceOf[KyuubiStatement]
        statement.executePython("l = [('Alice', 1)]")
        statement.executePython("df = spark.createDataFrame(l)")
        val errorMsg = intercept[KyuubiSQLException] {
          statement.executePython("%json df")
        }.getMessage
        assert(errorMsg.contains("Object of type DataFrame is not JSON serializable"))

        statement.executePython("df = spark.createDataFrame(l).collect()")
        val result = statement.executePython("%json df")
        assert(result.next())
        assert(result.getString("output") == "{\"application/json\":[[\"Alice\",1]]}")
      })
    }
  }

  test("Support to cancel Spark python operation") {
    checkPythonRuntimeAndVersion()
    withMultipleConnectionJdbcStatement()({ stmt =>
      val statement = stmt.asInstanceOf[KyuubiStatement]
      statement.executeQuery("SET kyuubi.operation.language=PYTHON")
      val code1 =
        """
          |i = 0
          |i
          |""".stripMargin
      val resultSet1 = statement.executeQuery(code1)
      assert(resultSet1.next())
      assert(resultSet1.getString("status") === "ok")
      assert(resultSet1.getString("output") === "0")
      val code2 =
        """
          |import time
          |while True:
          |   i +=1
          |   time.sleep(1)
          |""".stripMargin
      statement.executeAsync(code2)
      statement.cancel()

      val code3 =
        """
          |i
          |""".stripMargin
      val resultSet3 = statement.executeQuery(code3)
      assert(resultSet3.next())
      assert(resultSet3.getString("status") === "ok")
      assert(resultSet3.getString("output").toInt > 0)
    })
  }

  private def runPySparkTest(
      pyCode: String,
      output: String): Unit = {
    checkPythonRuntimeAndVersion()
    withMultipleConnectionJdbcStatement()({ statement =>
      statement.executeQuery("SET kyuubi.operation.language=python")
      val resultSet = statement.executeQuery(pyCode)
      assert(resultSet.next())
      assert(resultSet.getString("output") === output)
      assert(resultSet.getString("status") === "ok")
    })
  }

  private def checkPythonRuntimeAndVersion(): Unit = {
    val code =
      """
        |import sys
        |print(".".join(map(str, sys.version_info[:2])))
        |""".stripMargin
    withTempPyFile(code) {
      pyfile: String =>
        val pythonVersion = s"python3 $pyfile".!!.toDouble
        assert(pythonVersion > 3.0, "required python version > 3.0")
    }
  }

  private def withTempPyFile(code: String)(op: String => Unit): Unit = {
    val tempPyFile = Files.createTempFile("", ".py").toFile
    try {
      new PrintWriter(tempPyFile) {
        write(code)
        close()
      }
      op(tempPyFile.getPath)
    } finally {
      Files.deleteIfExists(tempPyFile.toPath)
    }
  }
}
