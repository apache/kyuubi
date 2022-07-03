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

package org.apache.kyuubi.session

import java.net.URI

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf

class KyuubiSessionManagerSuite extends KyuubiFunSuite {
  test("local dir allow list") {
    val conf = KyuubiConf().set(KyuubiConf.SESSION_LOCAL_DIR_ALLOW_LIST, Seq("/apache/kyuubi"))
    val sessionManager = new KyuubiSessionManager()
    sessionManager.initialize(conf)

    val noLocalDirLimitSessionManager = new KyuubiSessionManager()
    noLocalDirLimitSessionManager.initialize(KyuubiConf())

    var uri = new URI("/apache/kyuubi/a.jar")
    sessionManager.checkSessionAccessPathURI(uri)
    noLocalDirLimitSessionManager.checkSessionAccessPathURI(uri)

    uri = new URI("/apache/kyuubijar")
    var e = intercept[KyuubiException](sessionManager.checkSessionAccessPathURI(uri))
    assert(e.getMessage.contains("to access by the session is not allowed"))
    noLocalDirLimitSessionManager.checkSessionAccessPathURI(uri)

    uri = new URI("hdfs:/apache/kyuubijar")
    sessionManager.checkSessionAccessPathURI(uri)
    noLocalDirLimitSessionManager.checkSessionAccessPathURI(uri)

    uri = new URI("path/to/kyuubijar")
    e = intercept[KyuubiException](sessionManager.checkSessionAccessPathURI(uri))
    assert(e.getMessage.contains("is a relative path and is not allowed"))
    noLocalDirLimitSessionManager.checkSessionAccessPathURI(uri)

    sessionManager.stop()
    noLocalDirLimitSessionManager.stop()
  }
}
