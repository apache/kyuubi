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

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KubernetesApplicationOperation.LABEL_KYUUBI_UNIQUE_KEY

class KyuubiApplicationManagerSuite extends KyuubiFunSuite {
  test("application access path") {
    val localDirLimitConf = KyuubiConf()
      .set(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST, Set("/apache/kyuubi"))
    val noLocalDirLimitConf = KyuubiConf()

    var path = "/apache/kyuubi/a.jar"
    KyuubiApplicationManager.checkApplicationAccessPath(path, localDirLimitConf)
    KyuubiApplicationManager.checkApplicationAccessPath(path, noLocalDirLimitConf)

    path = "/apache/kyuubijar"
    var e = intercept[KyuubiException] {
      KyuubiApplicationManager.checkApplicationAccessPath(path, localDirLimitConf)
    }
    assert(e.getMessage.contains("is not in the local dir allow list"))
    KyuubiApplicationManager.checkApplicationAccessPath(path, noLocalDirLimitConf)

    path = "/apache/kyuubi/../a.jar"
    e = intercept[KyuubiException] {
      KyuubiApplicationManager.checkApplicationAccessPath(path, localDirLimitConf)
    }
    assert(e.getMessage.contains("is not in the local dir allow list"))
    KyuubiApplicationManager.checkApplicationAccessPath(path, noLocalDirLimitConf)

    path = "hdfs:/apache/kyuubijar"
    KyuubiApplicationManager.checkApplicationAccessPath(path, localDirLimitConf)
    KyuubiApplicationManager.checkApplicationAccessPath(path, noLocalDirLimitConf)

    path = "path/to/kyuubijar"
    e = intercept[KyuubiException] {
      KyuubiApplicationManager.checkApplicationAccessPath(path, localDirLimitConf)
    }
    assert(e.getMessage.contains("please use absolute path"))
    KyuubiApplicationManager.checkApplicationAccessPath(path, noLocalDirLimitConf)

    var appConf = Map("spark.files" -> "/apache/kyuubi/jars/a.jar")
    KyuubiApplicationManager.checkApplicationAccessPaths(
      "SPARK",
      appConf,
      localDirLimitConf)
    KyuubiApplicationManager.checkApplicationAccessPaths(
      "SPARK_SQL",
      appConf,
      localDirLimitConf)
    KyuubiApplicationManager.checkApplicationAccessPaths(
      "SPARK",
      appConf,
      noLocalDirLimitConf)

    appConf = Map("spark.files" -> "/apache/jars/a.jar")
    intercept[KyuubiException] {
      KyuubiApplicationManager.checkApplicationAccessPaths(
        "SPARK",
        appConf,
        localDirLimitConf)
    }
  }

  test("Test kyuubi application Manager tag spark on kubernetes application") {
    val conf: KyuubiConf = KyuubiConf()
    val tag = "kyuubi-test-tag"
    KyuubiApplicationManager.tagApplication(
      tag,
      "SPARK",
      Some("k8s://https://kyuubi-test:8443"),
      conf)

    val kubernetesTag = conf.getOption("spark.kubernetes.driver.label." + LABEL_KYUUBI_UNIQUE_KEY)
    val yarnTag = conf.getOption("spark.yarn.tags")
    assert(kubernetesTag.nonEmpty && tag.equals(kubernetesTag.get))
    assert(yarnTag.isEmpty)
  }
}
