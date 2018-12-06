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

package yaooqinn.kyuubi.service

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

import yaooqinn.kyuubi.KyuubiSQLException

class ServiceExceptionSuite extends SparkFunSuite {
  test("service exception") {
    val msg = "no reason"
    val e1 = new KyuubiSQLException(msg)

    val e2 = new ServiceException(e1)
    val tStatus1 = KyuubiSQLException.toTStatus(e2)
    assert(tStatus1.isSetStatusCode)
    assert(tStatus1.getErrorMessage === e1.toString)
    assert(tStatus1.getSqlState === null)
    assert(tStatus1.getErrorCode === 0)
    assert(tStatus1.getInfoMessages === KyuubiSQLException.toString(e2).asJava)
    assert(e2.getMessage === e1.toString)
    assert(e2.getCause === e1)

    val e3 = new ServiceException(msg)
    assert(e3.getMessage === msg)
    assert(e3.getCause === null)
    val tStatus2 = KyuubiSQLException.toTStatus(e3)
    assert(tStatus2.isSetStatusCode)
    assert(tStatus2.getErrorMessage === msg)
    assert(tStatus2.getSqlState === null)
    assert(tStatus2.getErrorCode === 0)
    assert(tStatus2.getInfoMessages === KyuubiSQLException.toString(e3).asJava)

    val e4 = new ServiceException(msg, e1)
    assert(e4.getMessage === msg)
    assert(e4.getCause === e1)
    val tStatus3 = KyuubiSQLException.toTStatus(e4)
    assert(tStatus3.isSetStatusCode)
    assert(tStatus3.getErrorMessage === msg)
    assert(tStatus3.getSqlState === null)
    assert(tStatus3.getErrorCode === 0)
    assert(tStatus3.getInfoMessages === KyuubiSQLException.toString(e4).asJava)
  }
}
