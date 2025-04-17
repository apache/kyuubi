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

package org.apache.kyuubi.server.metadata.api

/**
 * The kubernetes metadata store.
 *
 * @param identifier the kubernetes unique identifier.
 * @param context the kubernetes context.
 * @param namespace the kubernetes namespace.
 * @param podName the kubernetes pod name.
 * @param appId the application id.
 * @param appName the application name.
 * @param appState the application state.
 * @param appError the application error diagnose.
 * @param createTime the metadata create time.
 * @param updateTime the metadata update time.
 */
case class KubernetesMetadata(
    identifier: String,
    context: Option[String],
    namespace: Option[String],
    podName: String,
    appId: String,
    appName: String,
    appState: String,
    appError: Option[String],
    createTime: Long = 0L,
    updateTime: Long = 0L)
