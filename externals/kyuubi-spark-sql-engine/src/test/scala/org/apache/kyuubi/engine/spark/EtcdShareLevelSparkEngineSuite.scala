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

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SHARE_LEVEL, ENGINE_SPARK_MAX_INITIAL_WAIT, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel.ShareLevel

trait EtcdShareLevelSparkEngineSuite
  extends ShareLevelSparkEngineTests with WithEtcdCluster {
  override def withKyuubiConf: Map[String, String] = {
    super.withKyuubiConf ++
      etcdConf ++ Map(
        ENGINE_SHARE_LEVEL.key -> shareLevel.toString,
        ENGINE_SPARK_MAX_LIFETIME.key -> "PT20s",
        ENGINE_SPARK_MAX_INITIAL_WAIT.key -> "0",
        ENGINE_CHECK_INTERVAL.key -> "PT5s")
  }
}

class EtcdConnectionLevelSparkEngineSuite extends EtcdShareLevelSparkEngineSuite {
  override def shareLevel: ShareLevel = ShareLevel.CONNECTION
}

class EtcdUserLevelSparkEngineSuite extends EtcdShareLevelSparkEngineSuite {
  override def shareLevel: ShareLevel = ShareLevel.USER
}
