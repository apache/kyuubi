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
    assert(conf.get(SERVER_PRINCIPAL) === None)
    assert(conf.get(KINIT_MAX_ATTEMPTS) === 10)
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

    conf.setIfMissing(FRONTEND_MIN_WORKER_THREADS, 2188)
    assert(conf.get(FRONTEND_MIN_WORKER_THREADS) === 2188)

    conf.unset(FRONTEND_MIN_WORKER_THREADS)
    assert(conf.get(FRONTEND_MIN_WORKER_THREADS) === 9)

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
    val conf = KyuubiConf().loadFileDefaults()

    assert(conf.getUserDefaults("kyuubi").getOption("spark.user.test").get === "a")
    assert(conf.getUserDefaults("userb").getOption("spark.user.test").get === "b")
    assert(conf.getUserDefaults("userc").getOption("spark.user.test").get === "c")
  }

  test("time config test") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT, 1000L)
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "1000")
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "  1000  ")
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "1000A")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_IDLE_TIMEOUT))
    assert(e.getMessage.contains("ISO-8601"))
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "  P1DT2H3.2S  ")

    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) ===
      Duration.ofDays(1)
        .plusHours(2)
        .plusSeconds(3)
        .plusMillis(200)
        .toMillis)
  }

  test(KyuubiConf.OPERATION_QUERY_TIMEOUT.key) {
    val kyuubiConf = KyuubiConf()
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT).isEmpty)
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT, 1000L)
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "1000")
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) ===  Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "  1000  ")
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "1000A")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_QUERY_TIMEOUT))
    assert(e.getMessage.contains("ISO-8601"))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "  P1DT2H3.2S  ")

    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) ===
      Some(Duration.ofDays(1)
        .plusHours(2)
        .plusSeconds(3)
        .plusMillis(200)
        .toMillis))

    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "0")
    val e1 = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_QUERY_TIMEOUT))
    assert(e1.getMessage.contains("must >= 1s if set"))
  }
}
