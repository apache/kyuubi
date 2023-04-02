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

package org.apache.kyuubi.service.authentication.ldap

import org.apache.kyuubi.KyuubiFunSuite

class QuerySuite extends KyuubiFunSuite {

  test("QueryBuilderFilter") {
    val q = Query.builder
      .filter("test <uid_attr>=<value> query")
      .map("uid_attr", "uid")
      .map("value", "Hello!")
      .build
    assert("test uid=Hello! query" === q.filter)
    assert(0 === q.controls.getCountLimit)
  }

  test("QueryBuilderLimit") {
    val q = Query.builder
      .filter("<key1>,<key2>")
      .map("key1", "value1")
      .map("key2", "value2")
      .limit(8)
      .build
    assert("value1,value2" === q.filter)
    assert(8 === q.controls.getCountLimit)
  }

  test("QueryBuilderReturningAttributes") {
    val q = Query.builder
      .filter("(query)")
      .returnAttribute("attr1")
      .returnAttribute("attr2")
      .build
    assert("(query)" === q.filter)
    assert(Array("attr1", "attr2") === q.controls.getReturningAttributes)
  }
}
