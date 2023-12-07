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
    boolean stream;

    String system;

    List<String> stop;

    @JsonProperty("disable_search")
    boolean disableSearch;

    @JsonProperty("enable_citation")
    boolean enableCitation;

    @JsonProperty("user_id")
    String userId;

    public static ChatCompletionRequestBuilder builder() {
        return new ChatCompletionRequestBuilder();
    }


    public ChatCompletionRequest(List<ChatMessage> messages, List<Function> functions, Double temperature, Double topP, Double presenceScore, boolean stream, String system, List<String> stop, boolean disableSearch, boolean enableCitation, String userId) {
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

    public List<ChatMessage> getMessages() {
        return messages;
    }

    public void setMessages(List<ChatMessage> messages) {
        this.messages = messages;
    }

    public List<Function> getFunctions() {
        return functions;
    }

    public void setFunctions(List<Function> functions) {
        this.functions = functions;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Double getTopP() {
        return topP;
    }
    @JsonProperty("top_p")
    public void setTopP(Double topP) {
        this.topP = topP;
    }

    public Double getPresenceScore() {
        return presenceScore;
    }
    @JsonProperty("penalty_score")
    public void setPresenceScore(Double presenceScore) {
        this.presenceScore = presenceScore;
    }

    public boolean isStream() {
        return stream;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public List<String> getStop() {
        return stop;
    }

    public void setStop(List<String> stop) {
        this.stop = stop;
    }

    public boolean isDisableSearch() {
        return disableSearch;
    }
    @JsonProperty("disable_search")
    public void setDisableSearch(boolean disableSearch) {
        this.disableSearch = disableSearch;
    }

    public boolean isEnableCitation() {
        return enableCitation;
    }
    @JsonProperty("enable_citation")
    public void setEnableCitation(boolean enableCitation) {
        this.enableCitation = enableCitation;
    }

    public String getUserId() {
        return userId;
    }
    @JsonProperty("user_id")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "ChatCompletionRequest{" +
                "messages=" + messages +
                ", functions=" + functions +
                ", temperature=" + temperature +
                ", topP=" + topP +
                ", presenceScore=" + presenceScore +
                ", stream=" + stream +
                ", system='" + system + '\'' +
                ", stop=" + stop +
                ", disableSearch=" + disableSearch +
                ", enableCitation=" + enableCitation +
                ", userId='" + userId + '\'' +
                '}';
    }

    public static class ChatCompletionRequestBuilder {

        private List<ChatMessage> messages;

        private List<Function> functions;

        private Double temperature;

        private Double topP;

        private Double presenceScore;

        private boolean stream;

        private String system;

        private List<String> stop;

        private boolean disableSearch;

        private  boolean enableCitation;

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
        public ChatCompletionRequestBuilder stream(boolean stream) {
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
        public ChatCompletionRequestBuilder disableSearch(boolean disableSearch) {
            this.disableSearch = disableSearch;
            return this;
        }

        @JsonProperty("enable_citation")
        public ChatCompletionRequestBuilder enableCitation(boolean enableCitation) {
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

        @Override
        public String toString() {
            return "ChatCompletionRequestBuilder{" +
                    "messages=" + messages +
                    ", functions=" + functions +
                    ", temperature=" + temperature +
                    ", topP=" + topP +
                    ", presenceScore=" + presenceScore +
                    ", stream=" + stream +
                    ", system='" + system + '\'' +
                    ", stop=" + stop +
                    ", disableSearch=" + disableSearch +
                    ", enableCitation=" + enableCitation +
                    ", userId='" + userId + '\'' +
                    '}';
        }
    }
}

