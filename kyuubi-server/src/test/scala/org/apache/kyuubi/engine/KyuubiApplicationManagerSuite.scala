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

package org.apache.kyuubi.engine

import java.net.URI

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf

class KyuubiApplicationManagerSuite extends KyuubiFunSuite {
  test("application access path uri") {
    val localDirLimitConf = KyuubiConf()
      .set(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST, Seq("/apache/kyuubi"))
    val noLocalDirLimitConf = KyuubiConf()

    var uri = new URI("/apache/kyuubi/a.jar")
    KyuubiApplicationManager.checkAccessPathURI(uri, localDirLimitConf)
    KyuubiApplicationManager.checkAccessPathURI(uri, noLocalDirLimitConf)

    uri = new URI("/apache/kyuubijar")
    var e = intercept[KyuubiException] {
      KyuubiApplicationManager.checkAccessPathURI(uri, localDirLimitConf)
    }
    assert(e.getMessage.contains("is not in the local dir allow list"))
    KyuubiApplicationManager.checkAccessPathURI(uri, noLocalDirLimitConf)

    uri = new URI("hdfs:/apache/kyuubijar")
    KyuubiApplicationManager.checkAccessPathURI(uri, localDirLimitConf)
    KyuubiApplicationManager.checkAccessPathURI(uri, noLocalDirLimitConf)

    uri = new URI("path/to/kyuubijar")
    e = intercept[KyuubiException] {
      KyuubiApplicationManager.checkAccessPathURI(uri, localDirLimitConf)
    }
    assert(e.getMessage.contains("please use absolute path"))
    KyuubiApplicationManager.checkAccessPathURI(uri, noLocalDirLimitConf)

    var appConf = Map("spark.files" -> "/apache/kyuubi/jars/a.jar")
    KyuubiApplicationManager.checkApplicationPathURI(
      "SPARK",
      appConf,
      localDirLimitConf)
    KyuubiApplicationManager.checkApplicationPathURI(
      "SPARK_SQL",
      appConf,
      localDirLimitConf)
    KyuubiApplicationManager.checkApplicationPathURI(
      "SPARK",
      appConf,
      noLocalDirLimitConf)

    appConf = Map("spark.files" -> "/apache/jars/a.jar")
    intercept[KyuubiException] {
      KyuubiApplicationManager.checkApplicationPathURI(
        "SPARK",
        appConf,
        localDirLimitConf)
    }
  }
}
