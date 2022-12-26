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

import java.io.PrintWriter
import java.nio.file.Files
import java.sql.SQLTimeoutException
import java.util.Properties

import scala.sys.process._

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.jdbc.hive.{KyuubiSQLException, KyuubiStatement}
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.tags.PySparkTest

@PySparkTest
class PySparkTests extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = Map.empty

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

  private def withTempPyFile(code: String)(op: (String) => Unit): Unit = {
    val tempPyFile = Files.createTempFile("", ".py").toFile
    try {
      new PrintWriter(tempPyFile) {
        write(code)
        close
      }
      op(tempPyFile.getPath)
    } finally {
      Files.delete(tempPyFile.toPath)
    }
  }
}
