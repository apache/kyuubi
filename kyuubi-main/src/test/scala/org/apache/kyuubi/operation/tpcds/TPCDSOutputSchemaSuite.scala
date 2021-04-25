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

import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{JDBCTestUtils, WithKyuubiServer}

// scalastyle:off line.size.limit
/**
 * To run this test suite:
 * {{{
 *   build/mvn -Dtest=none -DwildcardSuites=org.apache.kyuubi.operation.tpcds.TPCDSOutputSchemaSuite test
 *  }}}
 *
 * To re-generate golden files for this suite:
 * {{{
 *   KYUUBI_UPDATE=1 build/mvn -Dtest=none -DwildcardSuites=org.apache.kyuubi.operation.tpcds.TPCDSOutputSchemaSuite test
 * }}}
 */
// scalastyle:on line.size.limit
class TPCDSOutputSchemaSuite extends WithKyuubiServer with JDBCTestUtils with TPCDSHelper {
  override protected val conf: KyuubiConf = KyuubiConf()
  override protected def jdbcUrl: String = getJdbcUrl
  override def database: String = this.getClass.getSimpleName
  override def format: String = "hive OPTIONS(fileFormat='parquet')"
  private val queryNameReg = """([a-z]+)(\d+)""".r("head", "index")

  override def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE DATABASE IF NOT EXISTS $database")
      tables.foreach { table =>
        statement.execute(table.create)
      }
    }
  }

  override def afterAll(): Unit = {
    withJdbcStatement() { statement =>
      statement.execute(s"DROP DATABASE IF EXISTS $database CASCADE")
    }
    super.afterAll()
  }

  private val regenerateGoldenFiles = sys.env.get("KYUUBI_UPDATE").contains("1")
  protected val baseResourcePath: Path = {
    java.nio.file.Paths.get("src", "test", "resources")
  }

  private def fileToString(file: Path): String = {
    new String(Files.readAllBytes(file), Charset.forName("UTF-8"))
  }

  private def runQuery(query: String, goldenFile: Path): Unit = {
    withJdbcStatement() { statement =>
      statement.execute(s"USE $database")
      val result = statement.executeQuery(query)
      try {
        val columnTypes = (1 to result.getMetaData.getColumnCount).map { i =>
          s"${result.getMetaData.getColumnName(i)}:${result.getMetaData.getColumnTypeName(i)}"
        }.mkString("struct<", ",", ">\n")
        if (regenerateGoldenFiles) {
          Files.write(goldenFile, columnTypes.getBytes())
        }

        val expected = fileToString(goldenFile)
        assert(columnTypes === expected)
      } finally {
        result.close()
      }
    }
  }

  private def getQueryIndex(queryName: String): Int = queryName match {
    case queryNameReg(_, index) => index.toInt
  }

  private def runQueries(name: String): Unit = {
    val queriesRoot = Thread.currentThread().getContextClassLoader.getResource(name)
    val queries = Files.list(Paths.get(queriesRoot.toURI))
    import scala.collection.JavaConverters._

    val validQueries = queries.iterator().asScala.filter { query =>
      query.toFile.listFiles().exists(_.getName.endsWith(".sql"))
    }.toSeq.sortBy(q => getQueryIndex(q.getFileName.toString))

    validQueries.foreach { q =>
      test(name + "-" + q.getFileName.toString) {
        q.toFile.listFiles().filter(_.getName.endsWith(".sql")).foreach { qf =>
          val schemaFile = Paths.get(
            baseResourcePath.toFile.getAbsolutePath,
            name,
            q.getFileName.toString,
            s"${qf.getName.stripSuffix(".sql")}.output.schema"
          )
          val queryString = fileToString(qf.toPath)
          runQuery(queryString, schemaFile)
        }
      }
    }
  }

  runQueries("tpcds-1_4")
}
