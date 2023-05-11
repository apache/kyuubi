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
import scala.collection.immutable.List

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.{DISPATCHERS, SKIP_PARSING_PERMANENT_VIEW_ENABLED}
import org.apache.spark.kyuubi.lineage.SparkContextHelper
import org.apache.spark.sql.SparkListenerExtensionTest
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.atlas.AtlasEntityHelper.PROCESS_TYPE_STRING
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.isSparkVersionAtMost

class AtlasLineageDispatcherSuite extends KyuubiFunSuite with SparkListenerExtensionTest {
  val catalogName =
    if (isSparkVersionAtMost("3.1")) "org.apache.spark.sql.connector.InMemoryTableCatalog"
    else "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

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

  test("altas lineage capture: insert into select sql") {
    val mockAtlasClient = new MockAtlasClient()
    AtlasClient.setClient(mockAtlasClient)

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      spark.sql("create table test_table1(a string, b string)")
      spark.sql("insert into test_table1 select a, b from test_table0").collect()
      val expected = Lineage(
        List("default.test_table0"),
        List("default.test_table1"),
        List(
          ("default.test_table1.a", Set("default.test_table0.a")),
          ("default.test_table1.b", Set("default.test_table0.b"))))
      eventually(Timeout(5.seconds)) {
        assert(mockAtlasClient.getEntities != null && mockAtlasClient.getEntities.nonEmpty)
      }
      checkAtlasLineageEntity(mockAtlasClient.getEntities.head, expected)
    }

  }

  def checkAtlasLineageEntity(entity: AtlasEntity, expected: Lineage): Unit = {
    assert(entity.getTypeName == PROCESS_TYPE_STRING)

    val appId = SparkContextHelper.globalSparkContext.applicationId
    assert(entity.getAttribute("qualifiedName") == appId)
    assert(entity.getAttribute("name")
      == s"${SparkContextHelper.globalSparkContext.appName} $appId")
    assert(StringUtils.isNotBlank(entity.getAttribute("currUser").asInstanceOf[String]))
    assert(entity.getAttribute("executionId") != null)
    assert(StringUtils.isNotBlank(entity.getAttribute("details").asInstanceOf[String]))
    assert(StringUtils.isNotBlank(entity.getAttribute("sparkPlanDescription").asInstanceOf[String]))

    val inputs = entity.getAttribute("inputs")
      .asInstanceOf[util.Collection[AtlasObjectId]].asScala
      .map(_.getUniqueAttributes.get("qualifiedName"))
    val outputs = entity.getAttribute("outputs")
      .asInstanceOf[util.Collection[AtlasObjectId]].asScala
      .map(_.getUniqueAttributes.get("qualifiedName"))
    // Configured in atlas-application.properties
    val cluster = "test"
    assertResult(expected.inputTables.map(s => s"$s@$cluster"))(inputs)
    assertResult(expected.outputTables.map(s => s"$s@$cluster"))(outputs)
  }

  class MockAtlasClient() extends AtlasClient {
    private var _entities: Seq[AtlasEntity] = null

    override def send(entities: Seq[AtlasEntity]): Unit = {
      _entities = entities
    }

    def getEntities: Seq[AtlasEntity] = _entities
  }
}
