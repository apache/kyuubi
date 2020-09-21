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

package org.apache.kyuubi.cli

import java.util.Objects

import org.apache.kyuubi.KyuubiFunSuite

class HandleIdentifierSuite extends KyuubiFunSuite {

  test("HandleIdentifier") {
    val id1 = HandleIdentifier()
    val tid1 = id1.toTHandleIdentifier
    val id2 = HandleIdentifier(tid1)
    assert(id1 === id2)

    val id3 = HandleIdentifier(id1.publicId, id1.secretId)
    assert(id3 === id1)
    assert(id3.toString === id1.publicId.toString)
    assert(id3.hashCode() ===
      (Objects.hashCode(id1.publicId) + 31) * 31 + Objects.hashCode(id1.secretId))
    val id4 = HandleIdentifier()
    assert(id4 !== id1)
    assert(id4 !== new Integer(1))
  }
}
