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

package org.apache.kyuubi.service.authentication

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SASL_QOP

class SaslQOPSuite extends KyuubiFunSuite {

  test("sasl qop") {
    val conf = KyuubiConf(false)
    assert(conf.get(SASL_QOP) === SaslQOP.AUTH.toString)
    SaslQOP.values.foreach { q =>
      conf.set(SASL_QOP, q.toString)
      assert(SaslQOP.withName(conf.get(SASL_QOP)) === q)
    }
    conf.set(SASL_QOP, "abc")
    val e = intercept[IllegalArgumentException](conf.get(SASL_QOP))
    assert(e.getMessage ===
      "The value of kyuubi.authentication.sasl.qop should be one of" +
        " auth, auth-conf, auth-int, but was abc")
  }

}
