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

package org.apache.kyuubi.plugin.spark.authz

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._

class PrivilegeObjectSuite extends KyuubiFunSuite {

  test("comparison of privilege objects") {
    val db1 = PrivilegeObject(PrivilegeObjectType.DATABASE, INSERT, "default1", "default1")
    val db11 = PrivilegeObject(PrivilegeObjectType.DATABASE, INSERT, "default1", "default1")
    val db2 = PrivilegeObject(PrivilegeObjectType.DATABASE, OTHER, "default2", "default2")
    assert(db1.compareTo(db11) === 0)
    assert(db1.compareTo(db2) === -1)

    val tbl1 = PrivilegeObject(PrivilegeObjectType.TABLE_OR_VIEW, INSERT, "default1", "default1")
    val tbl2 = PrivilegeObject(
      PrivilegeObjectType.TABLE_OR_VIEW, INSERT, "default1", "default1", Seq("ab"))
    val tbl22 = PrivilegeObject(
      PrivilegeObjectType.TABLE_OR_VIEW, OTHER, "default1", "default1", Seq("ab"))
    assert(db1.compareTo(tbl1) === -1)
    assert(tbl1.compareTo(tbl2) === -1)
    assert(tbl2.compareTo(tbl22) === 0)
  }
}
