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

package org.apache.spark.kyuubi.entity

/**
 * This object store the status messages about stages and tasks.
 * You can get the num of activeTasks or completedTasks or activeStages in the job.
 * @param jobId
 * @param statementId
 * @param taskNum
 * @param activeTasks
 * @param failedTasks
 * @param completedTasks
 * @param killedTasks
 * @param skippedTasks
 * @param skippedStages
 * @param activeStages
 * @param failedStages
 */
case class KyuubiJobInfo(
    jobId: String,
    statementId: String,
    var taskNum: Int,
    var activeTasks: Int,
    var failedTasks: Int,
    var completedTasks: Int,
    var killedTasks: Int,
    var skippedTasks: Int,
    var skippedStages: Int,
    var activeStages: Int,
    var failedStages: Int)
