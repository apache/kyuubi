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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite

class TPCHCatalogSuite extends KyuubiFunSuite {

  protected lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.planStats.enabled", "true")
      .getOrCreate()
  }

  test("get catalog name") {
    val catalog = new TPCHCatalog
    val catalogName = "test"
    catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
    assert(catalog._name == catalogName)
  }

  test("supports namespaces") {
    spark.sql("use tpch")
    assert(spark.sql(s"SHOW DATABASES").collect().length == 11)
    assert(spark.sql(s"SHOW NAMESPACES IN tpch.sf1").collect().length == 0)
  }

  test("tpch.sf1 count") {
    assert(spark.table("tpch.sf1.customer").count === 150000)
    assert(spark.table("tpch.sf1.orders").count === 1500000)
    assert(spark.table("tpch.sf1.lineitem").count === 6001215)
    assert(spark.table("tpch.sf1.part").count === 200000)
    assert(spark.table("tpch.sf1.partsupp").count === 800000)
    assert(spark.table("tpch.sf1.supplier").count === 10000)
    assert(spark.table("tpch.sf1.nation").count === 25)
    assert(spark.table("tpch.sf1.region").count === 5)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
