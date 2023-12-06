/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.chat.ernie.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ChatCompletionRequest {
    List<ChatMessage> messages;

    List<Function> functions;

    Double temperature;
    @JsonProperty("top_p")
    Double topP;

    @JsonProperty("penalty_score")
    Double presenceScore;

    Boolean stream;

    String system;

    List<String> stop;

    @JsonProperty("disable_search")
    Boolean disableSearch;

    @JsonProperty("enable_citation")
    Boolean enableCitation;

    @JsonProperty("user_id")
    String userId;

    public static ChatCompletionRequestBuilder builder() {
        return new ChatCompletionRequestBuilder();
    }


    public ChatCompletionRequest(List<ChatMessage> messages, List<Function> functions, Double temperature, Double topP, Double presenceScore, Boolean stream, String system, List<String> stop, Boolean disableSearch, Boolean enableCitation, String userId) {
        this.messages = messages;
        this.functions = functions;
        this.temperature = temperature;
        this.topP = topP;
        this.presenceScore = presenceScore;
        this.stream = stream;
        this.system = system;
        this.stop = stop;
        this.disableSearch = disableSearch;
        this.enableCitation = enableCitation;
        this.userId = userId;
    }

    public ChatCompletionRequest() {
    }

    public static class ChatCompletionRequestBuilder {

        private List<ChatMessage> messages;

        private List<Function> functions;

        private Double temperature;

        private Double topP;

        private Double presenceScore;

        private Boolean stream;

        private String system;

        private List<String> stop;

        private Boolean disableSearch;

        private  Boolean enableCitation;

        private String userId;

        ChatCompletionRequestBuilder() {

        }
        public ChatCompletionRequestBuilder messages(List<ChatMessage> messages) {
            this.messages = messages;
            return this;
        }

        public ChatCompletionRequestBuilder functions(List<Function> functions) {
            this.functions = functions;
            return this;
        }

        public ChatCompletionRequestBuilder temperature(Double temperature) {
            this.temperature = temperature;
            return this;
        }

        @JsonProperty("top_p")
        public ChatCompletionRequestBuilder topP(Double topP) {
            this.topP = topP;
            return this;
        }
        public ChatCompletionRequestBuilder stream(Boolean stream) {
            this.stream = stream;
            return this;
        }

        public ChatCompletionRequestBuilder system(String system) {
            this.system = system;
            return this;
        }

        public ChatCompletionRequestBuilder stop(List<String> stop) {
            this.stop = stop;
            return this;
        }


        @JsonProperty("presence_score")
        public ChatCompletionRequestBuilder presenceScore(Double presenceScore) {
            this.presenceScore = presenceScore;
            return this;
        }

        @JsonProperty("disable_search")
        public ChatCompletionRequestBuilder disableSearch(Boolean disableSearch) {
            this.disableSearch = disableSearch;
            return this;
        }

        @JsonProperty("enable_citation")
        public ChatCompletionRequestBuilder enableCitation(Boolean enableCitation) {
            this.enableCitation = enableCitation;
            return this;
        }

        public ChatCompletionRequestBuilder user(String userId) {
            this.userId = userId;
            return this;
        }

        public ChatCompletionRequest build() {
            return new ChatCompletionRequest(messages, functions, temperature, topP, presenceScore, stream, system, stop, disableSearch, enableCitation, userId);
        }


    }
}

