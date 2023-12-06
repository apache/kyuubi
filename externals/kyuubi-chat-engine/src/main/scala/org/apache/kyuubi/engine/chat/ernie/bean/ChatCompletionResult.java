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

import java.util.Objects;

public class ChatCompletionResult {
    String id;
    String object;
    long created;
    @JsonProperty("sentence_id")
    long sentenceId;
    @JsonProperty("is_end")
    Boolean isEnd;
    @JsonProperty("is_truncated")
    Boolean isTruncated;
    @JsonProperty("finish_reason")
    String finishReason;
    @JsonProperty("search_info")
    SearchInfo searchInfo;
    String result;
    @JsonProperty("need_clear_history")
    Boolean needClearHistory;
    @JsonProperty("ban_round")
    long banRound;
    Usage usage;
    @JsonProperty("function_call")
    FunctionCall functionCall;

    public ChatCompletionResult() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public long getSentenceId() {
        return sentenceId;
    }

    public void setSentenceId(long sentenceId) {
        this.sentenceId = sentenceId;
    }

    public Boolean getEnd() {
        return isEnd;
    }

    public void setEnd(Boolean end) {
        isEnd = end;
    }

    public Boolean getTruncated() {
        return isTruncated;
    }

    public void setTruncated(Boolean truncated) {
        isTruncated = truncated;
    }

    public String getFinishReason() {
        return finishReason;
    }

    public void setFinishReason(String finishReason) {
        this.finishReason = finishReason;
    }

    public SearchInfo getSearchInfo() {
        return searchInfo;
    }

    public void setSearchInfo(SearchInfo searchInfo) {
        this.searchInfo = searchInfo;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public Boolean getNeedClearHistory() {
        return needClearHistory;
    }

    public void setNeedClearHistory(Boolean needClearHistory) {
        this.needClearHistory = needClearHistory;
    }

    public long getBanRound() {
        return banRound;
    }

    public void setBanRound(long banRound) {
        this.banRound = banRound;
    }

    public Usage getUsage() {
        return usage;
    }

    public void setUsage(Usage usage) {
        this.usage = usage;
    }

    public FunctionCall getFunctionCall() {
        return functionCall;
    }

    public void setFunctionCall(FunctionCall functionCall) {
        this.functionCall = functionCall;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChatCompletionResult that = (ChatCompletionResult) o;
        return created == that.created && sentenceId == that.sentenceId && banRound == that.banRound && Objects.equals(id, that.id) && Objects.equals(object, that.object) && Objects.equals(isEnd, that.isEnd) && Objects.equals(isTruncated, that.isTruncated) && Objects.equals(finishReason, that.finishReason) && Objects.equals(searchInfo, that.searchInfo) && Objects.equals(result, that.result) && Objects.equals(needClearHistory, that.needClearHistory) && Objects.equals(usage, that.usage) && Objects.equals(functionCall, that.functionCall);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, object, created, sentenceId, isEnd, isTruncated, finishReason, searchInfo, result, needClearHistory, banRound, usage, functionCall);
    }
}
