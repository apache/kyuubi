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

package org.apache.kyuubi.spark.connector.hive

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.EXTERNAL_CATALOG_SHARE_POLICY

class ExternalCatalogPoolSuite extends KyuubiHiveTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    ExternalCatalogManager.reset()
  }

  test("test configuration for externalCatalog share policy") {
    withSparkSession() { spark =>
      val pool1 = ExternalCatalogManager.getOrCreate(spark)
      val pool2 = ExternalCatalogManager.getOrCreate(spark)
      assert(pool1.isInstanceOf[OneForAllPolicyManager])
      assert(pool1 == pool2)
    }

    ExternalCatalogManager.reset()
    withSparkSession(Map(EXTERNAL_CATALOG_SHARE_POLICY.key -> "ONE_FOR_ONE")) { spark =>
      val pool1 = ExternalCatalogManager.getOrCreate(spark)
      val pool2 = ExternalCatalogManager.getOrCreate(spark)
      assert(pool1.isInstanceOf[OneForOnePolicyManager.type])
      assert(pool1 == pool2)
    }
  }

  test("ALL policy: external catalog is shared globally " +
    "with the HiveCatalogs with the same catalogName") {
    withSparkSession(Map(
      EXTERNAL_CATALOG_SHARE_POLICY.key -> "ONE_FOR_ALL")) { spark =>
      val catalog1 =
        Ticket("catalog1", spark.sparkContext.getConf, spark.sessionState.newHadoopConf())
      val catalog2 =
        Ticket("catalog2", spark.sparkContext.getConf, spark.sessionState.newHadoopConf())
      val pool = ExternalCatalogManager.getOrCreate(spark)
      val externalCatalog1 = pool.take(catalog1)
      val externalCatalog2 = pool.take(catalog2)

      assert(externalCatalog1 != externalCatalog2)
      (1 to 10).foreach { id =>
        assert(pool.take(catalog1) == externalCatalog1)
      }

      (1 to 10).foreach { id =>
        assert(pool.take(catalog2) == externalCatalog2)
      }
    }
  }
}
