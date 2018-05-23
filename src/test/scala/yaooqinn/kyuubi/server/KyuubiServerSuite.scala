/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.server

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}

class KyuubiServerSuite extends SparkFunSuite {

  test("testSetupCommonConfig") {
    val conf = new SparkConf(true)
    KyuubiServer.setupCommonConfig(conf)
    val name = "spark.app.name"
    assert(conf.get(name) === "KyuubiServer")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.MULTIPLE_CONTEXTS) ===
      KyuubiSparkUtil.MULTIPLE_CONTEXTS_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.CATALOG_IMPL) === KyuubiSparkUtil.CATALOG_IMPL_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.DEPLOY_MODE) === KyuubiSparkUtil.DEPLOY_MODE_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.METASTORE_JARS) !== "builtin")
    assert(conf.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf.get(KyuubiSparkUtil.SPARK_LOCAL_DIR)
      .startsWith(System.getProperty("java.io.tmpdir")))
    val foo = "spark.foo"
    val e = intercept[NoSuchElementException](conf.get(foo))
    assert(e.getMessage === foo)
    val bar = "bar"
    val conf2 = new SparkConf(loadDefaults = true)
      .set(name, "test")
      .set(foo, bar)
      .set(KyuubiSparkUtil.SPARK_UI_PORT, "1234")
    KyuubiServer.setupCommonConfig(conf2)
    assert(conf.get(name) === "KyuubiServer") // app name will be overwritten
    assert(conf2.get(KyuubiSparkUtil.SPARK_UI_PORT) === KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
    assert(conf2.get(foo) === bar)
  }
}
