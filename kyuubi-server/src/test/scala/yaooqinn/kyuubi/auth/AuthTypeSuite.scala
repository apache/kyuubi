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

package yaooqinn.kyuubi.auth

import org.apache.spark.SparkFunSuite

import yaooqinn.kyuubi.service.ServiceException

class AuthTypeSuite extends SparkFunSuite {

  test("test name") {
    assert(new AuthType {}.name === "")
    assert(AuthType.NONE.name === "NONE")
    assert(AuthType.KERBEROS.name === "KERBEROS")
    assert(AuthType.NOSASL.name === "NOSASL")
  }

  test("to auth type") {
    assert(AuthType.toAuthType("NONE") === AuthType.NONE)
    assert(AuthType.toAuthType("KERBEROS") === AuthType.KERBEROS)
    assert(AuthType.toAuthType("NOSASL") === AuthType.NOSASL)
    intercept[ServiceException](AuthType.toAuthType("ELSE"))
  }
}
