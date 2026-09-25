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

  test("QueryBuilderFilter renders template into filterString and parses Filter") {
    val q = Query.builder
      .filter("(uid=<value>)")
      .map("value", "alice")
      .build
    // filterString holds the rendered template; filter is the parsed UnboundID Filter object.
    assert("(uid=alice)" === q.filterString)
    assert(q.filter != null)
    assert("(uid=alice)" === q.filter.toString)
    assert(0 === q.sizeLimit)
  }

  test("QueryBuilderLimit") {
    val q = Query.builder
      .filter("(uid=<value>)")
      .map("value", "alice")
      .limit(8)
      .build
    assert("(uid=alice)" === q.filterString)
    assert(8 === q.sizeLimit)
  }

  test("QueryBuilderReturningAttributes") {
    val q = Query.builder
      .filter("(objectClass=*)")
      .returnAttribute("attr1")
      .returnAttribute("attr2")
      .build
    assert("(objectClass=*)" === q.filterString)
    assert(Seq("attr1", "attr2") === q.attributes)
  }

  test("build throws IllegalStateException for non-LDAP filter string") {
    intercept[IllegalStateException] {
      Query.builder
        .filter("not a valid ldap filter")
        .build
    }
  }
}
