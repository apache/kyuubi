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
    boolean isEnd;
    @JsonProperty("is_truncated")
    boolean isTruncated;
    @JsonProperty("finish_reason")
    String finishReason;
    @JsonProperty("search_info")
    SearchInfo searchInfo;
    String result;
    @JsonProperty("need_clear_history")
    boolean needClearHistory;
    @JsonProperty("ban_round")
    long banRound;
    Usage usage;
    @JsonProperty("function_call")
    FunctionCall functionCall;

    @JsonProperty("error_msg")
    String errorMsg;

    @JsonProperty("error_code")
    int errorCode;

    public ChatCompletionResult() {
    }

    public ChatCompletionResult(String id, String object, long created, long sentenceId, boolean isEnd, boolean isTruncated, String finishReason, SearchInfo searchInfo, String result, boolean needClearHistory, long banRound, Usage usage, FunctionCall functionCall, String errorMsg, int errorCode) {
        this.id = id;
        this.object = object;
        this.created = created;
        this.sentenceId = sentenceId;
        this.isEnd = isEnd;
        this.isTruncated = isTruncated;
        this.finishReason = finishReason;
        this.searchInfo = searchInfo;
        this.result = result;
        this.needClearHistory = needClearHistory;
        this.banRound = banRound;
        this.usage = usage;
        this.functionCall = functionCall;
        this.errorMsg = errorMsg;
        this.errorCode = errorCode;
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
    @JsonProperty("sentence_id")
    public void setSentenceId(long sentenceId) {
        this.sentenceId = sentenceId;
    }

    public boolean getEnd() {
        return isEnd;
    }
    @JsonProperty("is_end")
    public void setEnd(boolean end) {
        isEnd = end;
    }

    public boolean getTruncated() {
        return isTruncated;
    }
    @JsonProperty("is_truncated")
    public void setTruncated(boolean truncated) {
        isTruncated = truncated;
    }

    public String getFinishReason() {
        return finishReason;
    }
    @JsonProperty("finish_reason")
    public void setFinishReason(String finishReason) {
        this.finishReason = finishReason;
    }

    public SearchInfo getSearchInfo() {
        return searchInfo;
    }
    @JsonProperty("search_info")
    public void setSearchInfo(SearchInfo searchInfo) {
        this.searchInfo = searchInfo;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public boolean getNeedClearHistory() {
        return needClearHistory;
    }
    @JsonProperty("need_clear_history")
    public void setNeedClearHistory(boolean needClearHistory) {
        this.needClearHistory = needClearHistory;
    }

    public long getBanRound() {
        return banRound;
    }
    @JsonProperty("ban_round")
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
    @JsonProperty("function_call")
    public void setFunctionCall(FunctionCall functionCall) {
        this.functionCall = functionCall;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
    @JsonProperty("error_msg")
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public int getErrorCode() {
        return errorCode;
    }
    @JsonProperty("error_code")
    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return "ChatCompletionResult{" +
                "id='" + id + '\'' +
                ", object='" + object + '\'' +
                ", created=" + created +
                ", sentenceId=" + sentenceId +
                ", isEnd=" + isEnd +
                ", isTruncated=" + isTruncated +
                ", finishReason='" + finishReason + '\'' +
                ", searchInfo=" + searchInfo +
                ", result='" + result + '\'' +
                ", needClearHistory=" + needClearHistory +
                ", banRound=" + banRound +
                ", usage=" + usage +
                ", functionCall=" + functionCall +
                ", errorMsg='" + errorMsg + '\'' +
                ", errorCode=" + errorCode +
                '}';
    }
}
