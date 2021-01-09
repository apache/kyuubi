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

package org.apache.kyuubi

import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException}

import org.apache.hive.service.rpc.thrift.TStatusCode

class KyuubiSQLExceptionSuite extends KyuubiFunSuite {

  test("KyuubiSQLException") {
    val msg0 = "this is just a dummy msg 0"
    val msg1 = "this is just a dummy msg 1"
    val msg2 = "this is just a dummy msg 2"

    val e0 = new KyuubiException(msg0)
    val e1 = new KyuubiException(msg1, e0)
    val e2 = new KyuubiSQLException(msg2, e1)
    assert(e2.toTStatus === KyuubiSQLException.toTStatus(e2))

    val e3 = KyuubiSQLException(e2.toTStatus)
    assert(e3.getMessage === e2.getMessage)
    assert(e3.getStackTrace === e2.getStackTrace)
    assert(e3.getCause.getMessage === e1.getMessage)
    assert(e3.getCause.getCause.getMessage === e0.getMessage)

    val ts0 = KyuubiSQLException.toTStatus(e0)
    assert(ts0.getStatusCode === TStatusCode.ERROR_STATUS)
    assert(ts0.getErrorMessage === msg0)
    assert(ts0.getInfoMessages.get(0).startsWith("*"))

    val e4 = KyuubiSQLException(ts0)
    assert(e4.getMessage === msg0)
    assert(e4.getCause.getStackTrace === e0.getStackTrace)

    val e5 = KyuubiSQLException(e0)
    assert(e5.getMessage === msg0)
    assert(e5.getCause === e0)
  }

  test("find the root cause") {
    val theCause = new RuntimeException("this is just a dummy message but shall be seen")
    val ite1 = new InvocationTargetException(theCause)
    val ute1 = new UndeclaredThrowableException(ite1)
    val ute2 = new UndeclaredThrowableException(ute1)
    val ite2 = new InvocationTargetException(ute2)
    val ke = KyuubiSQLException(ite2)
    assert(ke.getMessage == theCause.getMessage)
    assert(ke.getCause == theCause)
  }
}
