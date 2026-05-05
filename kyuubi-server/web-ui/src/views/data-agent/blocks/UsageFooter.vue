<!--
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
-->

<!-- Token usage footer: chip shows last context size (↑) and total turn output (↓);
     tooltip breaks down accumulated vs last for the full picture. -->
<template>
  <div class="usage-footer">
    <el-tooltip
      :content="
        $t('data_agent.token_usage_tooltip', {
          lastPrompt: usage.lastPrompt.toLocaleString(),
          lastCompletion: usage.lastCompletion.toLocaleString()
        })
      "
      placement="top"
      :show-after="200">
      <span class="usage-chip">
        <span class="usage-segment">
          <span class="usage-arrow">↑</span>
          <span class="usage-label">{{ $t('data_agent.tokens_in') }}</span>
          <span class="usage-num">{{ formatTokens(usage.lastPrompt) }}</span>
        </span>
        <span class="usage-divider">·</span>
        <span class="usage-segment">
          <span class="usage-arrow">↓</span>
          <span class="usage-label">{{ $t('data_agent.tokens_out') }}</span>
          <span class="usage-num">{{
            formatTokens(usage.accumulatedCompletion)
          }}</span>
        </span>
      </span>
    </el-tooltip>
  </div>
</template>

<script lang="ts" setup>
  import type { TokenUsage } from '../types'
  import { formatTokens } from '../utils/format'

  defineProps<{
    usage: TokenUsage
  }>()
</script>

<style lang="scss" scoped>
  .usage-footer {
    margin-top: 4px;
    display: flex;
    justify-content: flex-start;
  }
  .usage-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: #909399;
    padding: 2px 8px;
    border-radius: 4px;
    cursor: default;

    &:hover {
      background: #f5f7fa;
      color: #606266;
    }
  }
  .usage-segment {
    display: inline-flex;
    align-items: center;
    gap: 3px;
  }
  .usage-arrow {
    color: #c0c4cc;
    font-size: 10px;
    font-weight: 600;
  }
  .usage-label {
    font-size: 11px;
    color: #b1b6bd;
    text-transform: lowercase;
  }
  .usage-num {
    font-family: 'SFMono-Regular', Consolas, monospace;
    color: #606266;
  }
  .usage-divider {
    color: #dcdfe6;
  }
</style>
