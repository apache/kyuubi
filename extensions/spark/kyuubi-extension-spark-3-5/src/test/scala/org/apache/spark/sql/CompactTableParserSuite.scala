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
package org.apache.spark.sql

import org.apache.kyuubi.sql.SparkKyuubiSparkSQLParser
import org.apache.kyuubi.sql.compact.{CompactTableOptions, CompactTableStatement, RecoverCompactTableStatement}

import scala.util.Random

class CompactTableParserSuite extends KyuubiSparkSQLExtensionTest {

  test("parse compact table statement without target size") {
    val statement = s"COMPACT TABLE db1.t1"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === None)
    assert(CompactTableOptions.CleanupStagingFolder === compactTableStatement.options)
  }

  test("parse compact table statement with target size") {
    val targetSize = new Random(1).nextInt(256) + 1
    val statement = s"COMPACT TABLE db1.t1 INTO ${targetSize} MB"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === Some(targetSize))
    assert(CompactTableOptions.CleanupStagingFolder === compactTableStatement.options)
  }

  test("parse compact table statement with retain options") {
    val targetSize = new Random(1).nextInt(256) + 1
    val statement = s"COMPACT TABLE db1.t1 INTO ${targetSize} MB retain"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === Some(targetSize))
    assert(CompactTableOptions.RetainStagingFolder === compactTableStatement.options)
  }

  test("parse compact table statement with retain options, without target size") {
    val statement = s"COMPACT TABLE db1.t1 retain"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === None)
    assert(CompactTableOptions.RetainStagingFolder === compactTableStatement.options)
  }

  test("parse compact table statement with list options") {
    val targetSize = new Random(1).nextInt(256) + 1
    val statement = s"COMPACT TABLE db1.t1 INTO ${targetSize} MB list"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === Some(targetSize))
    assert(CompactTableOptions.DryRun === compactTableStatement.options)
  }

  test("parse compact table statement with list options, without target size") {
    val statement = s"COMPACT TABLE db1.t1 list"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[CompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[CompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
    assert(compactTableStatement.targetSizeInMB === None)
    assert(CompactTableOptions.DryRun === compactTableStatement.options)
  }

  test("parse recover compact table statement") {
    val statement = s"RECOVER COMPACT TABLE db1.t1"
    val parser = new SparkKyuubiSparkSQLParser(spark.sessionState.sqlParser)
    val parsed = parser.parsePlan(statement)
    assert(parsed.isInstanceOf[RecoverCompactTableStatement])
    val compactTableStatement = parsed.asInstanceOf[RecoverCompactTableStatement]
    assert(compactTableStatement.tableParts === Seq("db1", "t1"))
  }
}
