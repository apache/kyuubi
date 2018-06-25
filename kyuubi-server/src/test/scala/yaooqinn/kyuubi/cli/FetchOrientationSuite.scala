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

package yaooqinn.kyuubi.cli

import org.apache.hive.service.cli.thrift.TFetchOrientation
import org.apache.spark.SparkFunSuite

class FetchOrientationSuite extends SparkFunSuite {

  test("fetch orientation basic tests") {
    assert(FetchOrientation.FETCH_NEXT.toTFetchOrientation === TFetchOrientation.FETCH_NEXT)
    assert(FetchOrientation.FETCH_FIRST.toTFetchOrientation === TFetchOrientation.FETCH_FIRST)
    assert(FetchOrientation.getFetchOrientation(TFetchOrientation.FETCH_FIRST) ===
      FetchOrientation.FETCH_FIRST)
    assert(FetchOrientation.getFetchOrientation(TFetchOrientation.FETCH_NEXT) ===
      FetchOrientation.FETCH_NEXT)
    assert(FetchOrientation.getFetchOrientation(TFetchOrientation.FETCH_LAST) ===
      FetchOrientation.FETCH_NEXT)
  }
}
