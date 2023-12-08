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

package org.apache.kyuubi.engine.chat.ernie.bean

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonProperty

case class ChatCompletionRequest(
    @BeanProperty @JsonProperty("messages") messages: java.util.List[ChatMessage],
    @BeanProperty @JsonProperty("functions") functions: java.util.List[Function] = null,
    @BeanProperty @JsonProperty("temperature") temperature: java.lang.Double = null,
    @BeanProperty @JsonProperty("top_p") topP: java.lang.Double = null,
    @BeanProperty @JsonProperty("penalty_score") presenceScore: java.lang.Double = null,
    @BeanProperty @JsonProperty("stream") stream: Boolean = false,
    @BeanProperty @JsonProperty("system") system: String = null,
    @BeanProperty @JsonProperty("stop") stop: java.util.List[String] = null,
    @BeanProperty @JsonProperty("disable_search") disableSearch: Boolean = false,
    @BeanProperty @JsonProperty("enable_citation") enableCitation: Boolean = false,
    @BeanProperty @JsonProperty("user_id") userId: String = null)
