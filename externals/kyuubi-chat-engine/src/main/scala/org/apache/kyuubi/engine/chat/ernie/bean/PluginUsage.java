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

public class PluginUsage {

    String name;

    @JsonProperty("parse_tokens")
    long parseTokens;

    @JsonProperty("abstract_tokens")
    long abstractTokens;

    @JsonProperty("search_tokens")
    long searchTokens;

    @JsonProperty("total_tokens")
    long totalTokens;

    public PluginUsage() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getParseTokens() {
        return parseTokens;
    }
    @JsonProperty("parse_tokens")
    public void setParseTokens(long parseTokens) {
        this.parseTokens = parseTokens;
    }

    public long getAbstractTokens() {
        return abstractTokens;
    }
    @JsonProperty("abstract_tokens")
    public void setAbstractTokens(long abstractTokens) {
        this.abstractTokens = abstractTokens;
    }

    public long getSearchTokens() {
        return searchTokens;
    }
    @JsonProperty("search_tokens")
    public void setSearchTokens(long searchTokens) {
        this.searchTokens = searchTokens;
    }

    public long getTotalTokens() {
        return totalTokens;
    }
    @JsonProperty("total_tokens")
    public void setTotalTokens(long totalTokens) {
        this.totalTokens = totalTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginUsage that = (PluginUsage) o;
        return parseTokens == that.parseTokens && abstractTokens == that.abstractTokens && searchTokens == that.searchTokens && totalTokens == that.totalTokens && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, parseTokens, abstractTokens, searchTokens, totalTokens);
    }

    @Override
    public String toString() {
        return "PluginUsage{" +
                "name='" + name + '\'' +
                ", parseTokens=" + parseTokens +
                ", abstractTokens=" + abstractTokens +
                ", searchTokens=" + searchTokens +
                ", totalTokens=" + totalTokens +
                '}';
    }
}
