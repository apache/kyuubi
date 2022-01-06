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

package org.apache.kyuubi.event.logger.elasticsearch

import org.apache.kyuubi.KyuubiFunSuite

class ElasticSearchEventIndexSuite extends KyuubiFunSuite {

  test("get event elasticsearch index") {
    val kyuubiEventIndex = "test-kyuubi-events"
    val eventIndex1 = new ElasticSearchEventIndex(null, Some(kyuubiEventIndex))
    assert(eventIndex1.getIndex("kyuubi_session") === kyuubiEventIndex)
    assert(eventIndex1.getIndex("other_event") === kyuubiEventIndex)

    val kyuubiSessionIndex = "test-kyuubi-sessions"
    val kyuubiOperationIndex = "test-kyuubi-kyuubi_operations"
    val indices2 = s"$kyuubiEventIndex,kyuubi_session:$kyuubiSessionIndex," +
      s"kyuubi_operation:$kyuubiOperationIndex"
    val eventIndex2 = new ElasticSearchEventIndex(null, Some(indices2))
    assert(eventIndex2.getIndex("kyuubi_session") === kyuubiSessionIndex)
    assert(eventIndex2.getIndex("kyuubi_operation") === kyuubiOperationIndex)
    assert(eventIndex2.getIndex("other_event") === kyuubiEventIndex)
  }


}
