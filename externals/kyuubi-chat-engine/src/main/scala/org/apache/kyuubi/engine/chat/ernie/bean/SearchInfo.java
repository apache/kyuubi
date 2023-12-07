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

public class SearchInfo {
    @JsonProperty("is_beset")
    long isBeset;
    @JsonProperty("rewrite_query")
    String rewriteQuery;
    @JsonProperty("search_results")
    List<SearchResult>  searchResults;

    public SearchInfo() {
    }

    public long getIsBeset() {
        return isBeset;
    }
    @JsonProperty("is_beset")
    public void setIsBeset(long isBeset) {
        this.isBeset = isBeset;
    }

    public String getRewriteQuery() {
        return rewriteQuery;
    }
    @JsonProperty("rewrite_query")
    public void setRewriteQuery(String rewriteQuery) {
        this.rewriteQuery = rewriteQuery;
    }

    public List<SearchResult> getSearchResults() {
        return searchResults;
    }
    @JsonProperty("search_results")
    public void setSearchResults(List<SearchResult> searchResults) {
        this.searchResults = searchResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchInfo that = (SearchInfo) o;
        return isBeset == that.isBeset && Objects.equals(rewriteQuery, that.rewriteQuery) && Objects.equals(searchResults, that.searchResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBeset, rewriteQuery, searchResults);
    }

    @Override
    public String toString() {
        return "SearchInfo{" +
                "isBeset=" + isBeset +
                ", rewriteQuery='" + rewriteQuery + '\'' +
                ", searchResults=" + searchResults +
                '}';
    }
}
