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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.scalatest.time.{Minutes, Span}

import org.apache.kyuubi.sql.KyuubiSQLConf
import org.apache.kyuubi.tags.SparkLocalClusterTest

@SparkLocalClusterTest
class FinalStageResourceManagerSuite extends KyuubiSparkSQLExtensionTest {

  override def sparkConf(): SparkConf = {
    // It is difficult to run spark in local-cluster mode when spark.testing is set.
    sys.props.remove("spark.testing")

    super.sparkConf().set("spark.master", "local-cluster[3, 1, 1024]")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.initialExecutors", "3")
      .set("spark.dynamicAllocation.minExecutors", "1")
      .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
      .set(KyuubiSQLConf.FINAL_STAGE_CONFIG_ISOLATION.key, "true")
      .set(KyuubiSQLConf.FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_ENABLED.key, "true")
  }

  test("[KYUUBI #5136][Bug] Final Stage hangs forever") {
    // Prerequisite to reproduce the bug:
    // 1. Dynamic allocation is enabled.
    // 2. Dynamic allocation min executors is 1.
    // 3. target executors < active executors.
    // 4. No active executor is left after FinalStageResourceManager killed executors.
    //    This is possible because FinalStageResourceManager retained executors may already be
    //    requested to be killed but not died yet.
    // 5. Final Stage required executors is 1.
    withSQLConf(
      (KyuubiSQLConf.FINAL_WRITE_STAGE_EAGERLY_KILL_EXECUTORS_KILL_ALL.key, "true")) {
      withTable("final_stage") {
        eventually(timeout(Span(10, Minutes))) {
          sql(
            "CREATE TABLE final_stage AS SELECT id, count(*) as num FROM (SELECT 0 id) GROUP BY id")
        }
        assert(FinalStageResourceManager.getAdjustedTargetExecutors(spark.sparkContext).get == 1)
      }
    }
  }
}
