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
import java.util.Objects;

public class Usage {
    @JsonProperty("prompt_tokens")
    long promptTokens;
    @JsonProperty("completion_tokens")
    long completionTokens;
    @JsonProperty("total_tokens")
    long totalTokens;

    List<PluginUsage> plugins;

    public Usage() {
    }

    public long getPromptTokens() {
        return this.promptTokens;
    }

    public long getCompletionTokens() {
        return this.completionTokens;
    }

    public long getTotalTokens() {
        return this.totalTokens;
    }

    @JsonProperty("prompt_tokens")
    public void setPromptTokens(long promptTokens) {
        this.promptTokens = promptTokens;
    }

    @JsonProperty("completion_tokens")
    public void setCompletionTokens(long completionTokens) {
        this.completionTokens = completionTokens;
    }

    @JsonProperty("total_tokens")
    public void setTotalTokens(long totalTokens) {
        this.totalTokens = totalTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Usage usage = (Usage) o;
        return promptTokens == usage.promptTokens && completionTokens == usage.completionTokens && totalTokens == usage.totalTokens && Objects.equals(plugins, usage.plugins);
    }

    @Override
    public int hashCode() {
        return Objects.hash(promptTokens, completionTokens, totalTokens, plugins);
    }
}
