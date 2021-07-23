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

package org.apache.kyuubi.engine.spark.monitor.entity

import org.apache.spark.scheduler.StageInfo

/**
 * This object is used for storing the basic data for stage.
 * StageInfo store the metrics in its running time.
 *
 * In this object, you can use jobId to get all stages that belong to this job and also
 * you can use statementId to get all stages.
 *
 * @param stageId
 * @param statementId
 * @param stageInfo
 */
// TODO: kyuubi-851: Thread safe consider: for statementInfo, jobInfo and stageInfo
case class KyuubiStageInfo(
    stageId: Int,
    statementId: String,
    stageInfo: StageInfo)
