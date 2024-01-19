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

import java.lang.{Double => JDouble}
import java.util.{List => JList}

import com.fasterxml.jackson.annotation.JsonProperty

case class ChatCompletionRequest(
    @JsonProperty("messages") messages: JList[ChatMessage],
    @JsonProperty("functions") functions: JList[Function] = null,
    @JsonProperty("temperature") temperature: JDouble = null,
    @JsonProperty("top_p") topP: JDouble = null,
    @JsonProperty("penalty_score") presenceScore: JDouble = null,
    @JsonProperty("stream") stream: Boolean = false,
    @JsonProperty("system") system: String = null,
    @JsonProperty("stop") stop: JList[String] = null,
    @JsonProperty("disable_search") disableSearch: Boolean = false,
    @JsonProperty("enable_citation") enableCitation: Boolean = false,
    @JsonProperty("user_id") userId: String = null)
