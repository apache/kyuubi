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

package org.apache.kyuubi.engine.spark

import org.apache.kyuubi.KyuubiFunSuite

class SparkSQLEngineSuite extends KyuubiFunSuite {

  private val namespace = "n" * 63
  private val podUid = "u" * 36

  test("[KYUUBI #3385] generate executor pod name prefix with user or UUID") {
    val userName1 = "/kyuubi_user+-*"
    val executorPodNamePrefix1 = SparkSQLEngine.generateExecutorPodNamePrefixForK8s(userName1)
    assert(executorPodNamePrefix1.contains("-kyuubi-user-"))

    val userName2 = "LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLong" +
      "LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLong" +
      "LongLongLongLongLongLongLongLongLongLongLongLongLongLongLongLong" +
      "LongLongLongLongLongLongLongLongLongLongLongLongName"
    val executorPodNamePrefix2 = SparkSQLEngine.generateExecutorPodNamePrefixForK8s(userName2)
    assert(!executorPodNamePrefix2.contains(userName2))
    assert(executorPodNamePrefix2.length <= SparkSQLEngine.EXECUTOR_POD_NAME_PREFIX_MAX_LENGTH)

    val userName3 = "l" * 160
    val executorPodNamePrefix3 =
      SparkSQLEngine.generateExecutorPodNamePrefixForK8s(userName3, namespace)
    val executorPodName3 = s"$executorPodNamePrefix3-exec-${Int.MaxValue}"
    assert(!executorPodNamePrefix3.contains(userName3))
    assert(podLogsDirectoryNameLength(namespace, executorPodName3) <= 253)
  }

  private def podLogsDirectoryNameLength(namespace: String, podName: String): Int = {
    s"${namespace}_${podName}_$podUid".length
  }
}
