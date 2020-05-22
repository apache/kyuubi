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

package org.apache.spark

class KyuubiConfSuite extends SparkFunSuite {

  test("set kyuubi defaults to spark conf") {
    val conf = new SparkConf()
    for (kv <- KyuubiConf.getAllDefaults) {
      conf.setIfMissing(kv._1, kv._2)
    }
    assert(conf.get(KyuubiConf.AUTHENTICATION_METHOD) === "NONE")
  }

  test("not set explicitly declared kyuubi confs to spark conf") {
    val conf = new SparkConf().set(KyuubiConf.AUTHENTICATION_METHOD, "KERBEROS")
    for (kv <- KyuubiConf.getAllDefaults) {
      conf.setIfMissing(kv._1, kv._2)
    }
    assert(conf.get(KyuubiConf.AUTHENTICATION_METHOD) === "KERBEROS")
  }

  test("load sys props") {
    System.setProperty("spark.kyuubi.authentication", "KERBEROS")
    val conf = new SparkConf()
    for (kv <- KyuubiConf.getAllDefaults) {
      conf.setIfMissing(kv._1, kv._2)
    }
    assert(conf.get(KyuubiConf.AUTHENTICATION_METHOD) === "KERBEROS")
  }

  test("register") {
    val e = intercept[IllegalArgumentException](
      KyuubiConf.register(KyuubiConf.AUTHENTICATION_METHOD))
    assert(e.getMessage.contains("spark.kyuubi.authentication has been registered"))
  }
}
