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

package org.apache.kyuubi.config

import java.time.Duration

import org.apache.kyuubi.KyuubiFunSuite

class KyuubiConfSuite extends KyuubiFunSuite {

  import KyuubiConf._

  test("kyuubi conf defaults") {
    val conf = new KyuubiConf()
    assert(conf.get(EMBEDDED_ZK_PORT) === 2181)
    assert(conf.get(EMBEDDED_ZK_TEMP_DIR).endsWith("embedded_zookeeper"))
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === Duration.ofHours(3).toMillis)
  }

  test("kyuubi conf w/ w/o no sys defaults") {
    val key = "kyuubi.conf.abc"
    System.setProperty(key, "xyz")
    assert(KyuubiConf(false).getOption(key).isEmpty)
    assert(KyuubiConf(true).getAll.contains(key))
  }

  test("load default config file") {
    val conf = KyuubiConf().loadFileDefaults()
    assert(conf.getOption("kyuubi.yes").get === "yes")
    assert(conf.getOption("spark.kyuubi.yes").get === "no")
  }


  test("set and unset conf") {
    val conf = new KyuubiConf()

    val key = "kyuubi.conf.abc"
    conf.set(key, "opq")
    assert(conf.getOption(key) === Some("opq"))

    conf.set(OPERATION_IDLE_TIMEOUT, 5L)
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === 5)

    conf.set(FRONTEND_BIND_HOST, "kentyao.org")
    assert(conf.get(FRONTEND_BIND_HOST).get === "kentyao.org")

    conf.setIfMissing(OPERATION_IDLE_TIMEOUT, 60L)
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === 5)

    conf.setIfMissing(EMBEDDED_ZK_PORT, 2188)
    assert(conf.get(EMBEDDED_ZK_PORT) === 2188)

    conf.unset(EMBEDDED_ZK_PORT)
    assert(conf.get(EMBEDDED_ZK_PORT) === 2181)

    conf.unset(key)
    assert(conf.getOption(key).isEmpty)

    val map = conf.getAllWithPrefix("kyuubi", "")
    assert(map(FRONTEND_BIND_HOST.key.substring(7)) === "kentyao.org")
    val map1 = conf.getAllWithPrefix("kyuubi", "operation")
    assert(map1(OPERATION_IDLE_TIMEOUT.key.substring(7)) === "PT0.005S")
    assert(map1.size === 1)
  }

  test("clone") {
    val conf = KyuubiConf()
    val key = "kyuubi.abc.conf"
    conf.set(key, "xyz")
    val cloned = conf.clone
    assert(conf !== cloned)
    assert(cloned.getOption(key).get === "xyz")
  }

  test("to spark prefixed conf") {
    val conf = KyuubiConf(false)
    assert(conf.toSparkPrefixedConf.isEmpty)
    assert(conf.set("kyuubi.kent", "yao").toSparkPrefixedConf("spark.kyuubi.kent") === "yao")
    assert(conf.set("spark.kent", "yao").toSparkPrefixedConf("spark.kent") === "yao")
    assert(conf.set("kent", "yao").toSparkPrefixedConf("spark.kent") === "yao")
    assert(conf.set("hadoop.kent", "yao").toSparkPrefixedConf("spark.hadoop.hadoop.kent") === "yao")
  }


  test("get user specific defaults") {
    val conf = KyuubiConf(false)
      .set("spark.user.test", "a")
      .set("___kent___.spark.user.test", "b")
      .set("___yao___.spark.user.test", "c")

    val all1 = conf.getUserDefaults("yaooqinn").getAll
    assert(all1.size === 1)
    assert(all1("spark.user.test") === "a")
    val all2 = conf.getUserDefaults("kent").getAll
    assert(all2.size === 1)
    assert(all2("spark.user.test") === "b")
    assert(conf.getUserDefaults("yao").getOption("spark.user.test").get === "c")
  }

}
