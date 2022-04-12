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
package org.apache.kyuubi.engine.hive.deploy

import org.apache.kyuubi.KyuubiFunSuite

class HiveSubmitArgumentsSuite extends KyuubiFunSuite {

  test("test arguments") {
    val args = Array[String](
      "--master",
      "local",
      "--deploy-mode",
      "cluster",
      "--verbose",
      "--proxy-user",
      "kyuubi")
    val hiveSubmitArguments = new HiveSubmitArguments(args)
    assert("local".equals(hiveSubmitArguments.master))
    assert("cluster".equals(hiveSubmitArguments.deployMode))
    assert(hiveSubmitArguments.verbose)
    assert(hiveSubmitArguments.proxyUserOpt.isDefined &&
      "kyuubi".equals(hiveSubmitArguments.proxyUserOpt.get))
  }

  test("specify deploy mode through args") {
    val args = Seq[String](
      "")

    val hiveSubmitArguments = new HiveSubmitArguments(args.toArray)
  }

}
