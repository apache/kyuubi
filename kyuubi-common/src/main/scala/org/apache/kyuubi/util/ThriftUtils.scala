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

package org.apache.kyuubi.util

import org.apache.hive.service.rpc.thrift.{TRow, TRowSet, TStatus, TStatusCode}

import org.apache.kyuubi.KyuubiSQLException

object ThriftUtils {

  def verifyTStatus(tStatus: TStatus): Unit = {
    if (tStatus.getStatusCode != TStatusCode.SUCCESS_STATUS) {
      throw KyuubiSQLException(tStatus)
    }
  }

  val EMPTY_ROW_SET = new TRowSet(0, new java.util.ArrayList[TRow](0))

}
