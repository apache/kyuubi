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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table => V2Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

class DataSourceV2RelationTableExtractorSuite extends AnyFunSuite with BeforeAndAfterAll {
// scalastyle:on

  private val skipConfKey = "spark.kyuubi.authz.skipCataloglessV2Relation.enabled"

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(getClass.getSimpleName)
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  private val extractor = new DataSourceV2RelationTableExtractor

  /**
   * A v2 table from a TableProvider without SupportsCatalogOptions, such as the
   * MongoTable created by `spark.read.format("mongodb")`. Spark's non-catalog
   * fallback leaves both catalog and identifier empty on the relation, and name()
   * is a synthetic descriptor rather than a resolvable identifier.
   */
  private class CatalogLessTable extends V2Table {
    override def name(): String = "MongoTable()"
    override def schema(): StructType = StructType(Seq(StructField("_id", StringType)))
    override def capabilities(): util.Set[TableCapability] =
      util.EnumSet.of(TableCapability.BATCH_READ)
  }

  private def cataloglessRelation: DataSourceV2Relation =
    DataSourceV2Relation.create(new CatalogLessTable, None, None)

  test("extract table name verbatim for catalog-less relation by default") {
    val maybeTable = extractor.apply(spark, cataloglessRelation)
    assert(maybeTable.nonEmpty)
    assert(maybeTable.get.table === "MongoTable()")
    assert(maybeTable.get.database.isEmpty)
  }

  test("skip relation without catalog and identifier when skip conf is enabled") {
    spark.conf.set(skipConfKey, "true")
    try {
      assert(extractor.apply(spark, cataloglessRelation).isEmpty)
    } finally {
      spark.conf.unset(skipConfKey)
    }
  }

  test("still extract the table when an identifier is present") {
    spark.conf.set(skipConfKey, "true")
    try {
      val identifier = Identifier.of(Array("some_db"), "some_table")
      val relation = DataSourceV2Relation.create(new CatalogLessTable, None, Some(identifier))
      val maybeTable = extractor.apply(spark, relation)
      assert(maybeTable.nonEmpty)
      assert(maybeTable.get.database === Some("some_db"))
    } finally {
      spark.conf.unset(skipConfKey)
    }
  }
}
