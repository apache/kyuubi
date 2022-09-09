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

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.{EXTERNAL_CATALOG_POOL_ENABLED, EXTERNAL_CATALOG_POOL_SIZE}

class ExternalCatalogPoolSuite extends KyuubiHiveTest {

  test("test configuration w/ & w/o enable externalCatalog pool") {
    withSparkSession() { spark =>
      val pool1 = ExternalCatalogPool.getOrCreate(spark)
      val pool2 = ExternalCatalogPool.getOrCreate(spark)
      assert(pool1.isInstanceOf[NoopPool])
      assert(pool1 == pool2)
    }

    ExternalCatalogPool.reset()
    withSparkSession(Map(EXTERNAL_CATALOG_POOL_ENABLED.key -> "true")) { spark =>
      val pool1 = ExternalCatalogPool.getOrCreate(spark)
      val pool2 = ExternalCatalogPool.getOrCreate(spark)
      assert(pool1.isInstanceOf[PriorityPool])
      assert(pool1 == pool2)
    }
  }

  test("pop up in order of last use") {
    withSparkSession(Map(
      EXTERNAL_CATALOG_POOL_ENABLED.key -> "true",
      EXTERNAL_CATALOG_POOL_SIZE.key -> "2")) { spark =>
      val catalog =
        Ticket("catalog1", spark.sparkContext.getConf, spark.sessionState.newHadoopConf())
      val pool = ExternalCatalogPool.getOrCreate(spark)

      // full of the pool
      val externalCatalog1 = pool.take(catalog)
      val externalCatalog2 = pool.take(catalog)

      assert(externalCatalog1 != externalCatalog2)
      (1 to 10).foreach { id =>
        if (id % 2 == 0) {
          assert(pool.take(catalog) == externalCatalog2)
        } else {
          assert(pool.take(catalog) == externalCatalog1)
        }
      }
    }
  }
}
