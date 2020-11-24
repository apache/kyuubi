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

package org.apache.kyuubi.service

import org.apache.kyuubi.KyuubiFunSuite

class ServiceUtilsSuite extends KyuubiFunSuite {

  test("test index or domain match") {
    assert(ServiceUtils.indexOfDomainMatch(null) === -1)
    val user1 = "Kent"
    assert(ServiceUtils.indexOfDomainMatch(user1) === -1)
    val user2 = "Kent/xxx"
    assert(ServiceUtils.indexOfDomainMatch(user2) === 4)
    val user3 = "Kent/////xxx"
    assert(ServiceUtils.indexOfDomainMatch(user3) === 4)
    val user4 = "Kent/@/@//xxx"
    assert(ServiceUtils.indexOfDomainMatch(user4) === 4)
    val user5 = "Kent/xxx@xxx"
    assert(ServiceUtils.indexOfDomainMatch(user5) === 4)
    val user6 = "Kent@xxx@xxx"
    assert(ServiceUtils.indexOfDomainMatch(user6) === 4)
    val user7 = "Kent/xxx/xxx"
    assert(ServiceUtils.indexOfDomainMatch(user7) === 4)
    val user8 = "Kent*xxx/xxx"
    assert(ServiceUtils.indexOfDomainMatch(user8) === 8)
  }

  test("get short name") {
    assert(ServiceUtils.getShortName(null) === null)
    assert(ServiceUtils.getShortName("") === "")
    assert(ServiceUtils.getShortName("kent") === "kent")
    assert(ServiceUtils.getShortName("kent/x") == "kent")
    assert(ServiceUtils.getShortName("kent@abc/c") === "kent")
    assert(ServiceUtils.getShortName("kent/abc@c") === "kent")
  }
}
