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

package org.apache.kyuubi.operation

import org.apache.hive.service.rpc.thrift.TFetchOrientation

import org.apache.kyuubi.KyuubiFunSuite

class FetchOrientationSuite extends KyuubiFunSuite {

  test("getFetchOrientation") {
    val get = FetchOrientation.getFetchOrientation _
    assert(get(TFetchOrientation.FETCH_ABSOLUTE) === FetchOrientation.FETCH_ABSOLUTE)
    assert(get(TFetchOrientation.FETCH_FIRST) === FetchOrientation.FETCH_FIRST)
    assert(get(TFetchOrientation.FETCH_LAST) === FetchOrientation.FETCH_LAST)
    assert(get(TFetchOrientation.FETCH_RELATIVE) === FetchOrientation.FETCH_RELATIVE)
    assert(get(TFetchOrientation.FETCH_PRIOR) === FetchOrientation.FETCH_PRIOR)
    assert(get(TFetchOrientation.FETCH_NEXT) === FetchOrientation.FETCH_NEXT)
  }

  test("toTFetchOrientation") {
    val to = FetchOrientation.toTFetchOrientation _
    assert(to(FetchOrientation.FETCH_ABSOLUTE) === TFetchOrientation.FETCH_ABSOLUTE)
    assert(to(FetchOrientation.FETCH_FIRST) === TFetchOrientation.FETCH_FIRST)
    assert(to(FetchOrientation.FETCH_LAST) === TFetchOrientation.FETCH_LAST)
    assert(to(FetchOrientation.FETCH_RELATIVE) === TFetchOrientation.FETCH_RELATIVE)
    assert(to(FetchOrientation.FETCH_PRIOR) === TFetchOrientation.FETCH_PRIOR)
    assert(to(FetchOrientation.FETCH_NEXT) === TFetchOrientation.FETCH_NEXT)
  }

}
