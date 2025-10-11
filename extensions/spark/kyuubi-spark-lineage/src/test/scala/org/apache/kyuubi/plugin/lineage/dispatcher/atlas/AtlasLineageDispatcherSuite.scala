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

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import java.util

import scala.collection.JavaConverters._

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.{DEFAULT_CATALOG, DISPATCHERS, SKIP_PARSING_PERMANENT_VIEW_ENABLED}
import org.apache.spark.kyuubi.lineage.SparkContextHelper
import org.apache.spark.sql.SparkListenerExtensionTest
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.atlas.AtlasEntityHelper.{buildColumnQualifiedName, buildTableQualifiedName, COLUMN_LINEAGE_TYPE, PROCESS_TYPE}

class AtlasLineageDispatcherSuite extends KyuubiFunSuite with SparkListenerExtensionTest {
  val catalogName = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      .set(DISPATCHERS.key, "ATLAS")
      .set(SKIP_PARSING_PERMANENT_VIEW_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("altas lineage capture: insert into select sql") {
    val mockAtlasClient = new MockAtlasClient()
    AtlasClient.setClient(mockAtlasClient)

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b int, c int)")
      spark.sql("create table test_table1(a string, d int)")
      spark.sql("insert into test_table1 select a, b + c as d from test_table0").collect()
      val expected = Lineage(
        List(s"$DEFAULT_CATALOG.default.test_table0"),
        List(s"$DEFAULT_CATALOG.default.test_table1"),
        List(
          (
            s"$DEFAULT_CATALOG.default.test_table1.a",
            Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
          (
            s"$DEFAULT_CATALOG.default.test_table1.d",
            Set(
              s"$DEFAULT_CATALOG.default.test_table0.b",
              s"$DEFAULT_CATALOG.default.test_table0.c"))))
      eventually(Timeout(5.seconds)) {
        assert(mockAtlasClient.getEntities != null && mockAtlasClient.getEntities.nonEmpty)
      }
      checkAtlasProcessEntity(mockAtlasClient.getEntities.head, expected)
      checkAtlasColumnLineageEntities(
        mockAtlasClient.getEntities.head,
        mockAtlasClient.getEntities.tail,
        expected)
    }

  }

  def checkAtlasProcessEntity(entity: AtlasEntity, expected: Lineage): Unit = {
    assert(entity.getTypeName == PROCESS_TYPE)

    val appId = SparkContextHelper.globalSparkContext.applicationId
    assert(entity.getAttribute("qualifiedName") == appId)
    assert(entity.getAttribute("name")
      == s"${SparkContextHelper.globalSparkContext.appName} $appId")
    assert(StringUtils.isNotBlank(entity.getAttribute("currUser").asInstanceOf[String]))
    assert(entity.getAttribute("executionId") != null)
    assert(StringUtils.isNotBlank(entity.getAttribute("details").asInstanceOf[String]))
    assert(StringUtils.isNotBlank(entity.getAttribute("sparkPlanDescription").asInstanceOf[String]))

    val inputs = entity.getRelationshipAttribute("inputs")
      .asInstanceOf[util.Collection[AtlasObjectId]].asScala.map(getQualifiedName)
    val outputs = entity.getRelationshipAttribute("outputs")
      .asInstanceOf[util.Collection[AtlasObjectId]].asScala.map(getQualifiedName)
    assertResult(expected.inputTables
      .flatMap(buildTableQualifiedName(_).toSeq))(inputs)
    assertResult(expected.outputTables
      .flatMap(buildTableQualifiedName(_).toSeq))(outputs)
  }

  def checkAtlasColumnLineageEntities(
      processEntity: AtlasEntity,
      entities: Seq[AtlasEntity],
      expected: Lineage): Unit = {
    assert(entities.size == expected.columnLineage.size)

    entities.zip(expected.columnLineage).foreach {
      case (entity, expectedLineage) =>
        assert(entity.getTypeName == COLUMN_LINEAGE_TYPE)
        val expectedQualifiedName =
          s"${processEntity.getAttribute("qualifiedName")}:" +
            s"${buildColumnQualifiedName(expectedLineage.column).get}"
        assert(entity.getAttribute("qualifiedName") == expectedQualifiedName)
        assert(entity.getAttribute("name") == expectedQualifiedName)

        val inputs = entity.getRelationshipAttribute("inputs")
          .asInstanceOf[util.Collection[AtlasObjectId]].asScala.map(getQualifiedName)
        assertResult(expectedLineage.originalColumns
          .flatMap(buildColumnQualifiedName(_).toSet))(inputs.toSet)

        val outputs = entity.getRelationshipAttribute("outputs")
          .asInstanceOf[util.Collection[AtlasObjectId]].asScala.map(getQualifiedName)
        assert(outputs.size == 1)
        assert(buildColumnQualifiedName(expectedLineage.column).toSeq.head == outputs.head)

        assert(getQualifiedName(entity.getRelationshipAttribute("process").asInstanceOf[
          AtlasObjectId]) == processEntity.getAttribute("qualifiedName"))
    }
  }

  // Pre-set cluster name for testing in `test/resources/atlas-application.properties`
  private val cluster = "test"

  def getQualifiedName(objId: AtlasObjectId): String = {
    objId.getUniqueAttributes.get("qualifiedName").asInstanceOf[String]
  }

  class MockAtlasClient() extends AtlasClient {
    private var _entities: Seq[AtlasEntity] = _

    override def send(entities: Seq[AtlasEntity]): Unit = {
      _entities = entities
    }

    def getEntities: Seq[AtlasEntity] = _entities

    override def close(): Unit = {}
  }
}
