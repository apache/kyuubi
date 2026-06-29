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

<template>
  <div class="approval-block">
    <div class="approval-header">
      <div class="approval-icon">
        <el-icon :size="16" color="#e6a23c"><Warning /></el-icon>
      </div>
      <div class="approval-info">
        <span class="approval-title">{{
          $t('data_agent.approval_required')
        }}</span>
        <span class="approval-tool">
          <span class="tool-name-inline">{{ block.name }}</span>
          <el-tag size="small" type="danger" effect="plain">{{
            block.riskLevel
          }}</el-tag>
        </span>
      </div>
    </div>
    <div v-if="block.args" class="approval-args">
      <div class="tool-section-label">{{ $t('data_agent.arguments') }}</div>
      <pre class="tool-pre">{{ formatArgsForDisplay(block.args) }}</pre>
    </div>
    <div class="approval-actions">
      <el-button
        type="primary"
        size="small"
        :icon="Check"
        :loading="approvingRequestId === block.requestId"
        :disabled="!!approvingRequestId"
        @click="emit('approve', block.requestId!)">
        {{ $t('data_agent.approve') }}
      </el-button>
      <el-button
        type="danger"
        size="small"
        plain
        :icon="Close"
        :disabled="!!approvingRequestId"
        @click="emit('deny', block.requestId!)">
        {{ $t('data_agent.deny') }}
      </el-button>
    </div>
  </div>
</template>

<script lang="ts" setup>
  import { Warning, Check, Close } from '@element-plus/icons-vue'
  import type { ChatBlock } from '../types'
  import { formatArgsForDisplay } from '../utils/format'

  defineProps<{
    block: ChatBlock
    approvingRequestId?: string
  }>()

  const emit = defineEmits<{
    (e: 'approve', requestId: string): void
    (e: 'deny', requestId: string): void
  }>()
</script>

<style lang="scss" scoped>
  .approval-block {
    padding: 12px 14px;
    background: #fffbe6;
    border-left: 3px solid #e6a23c;
  }
  .approval-header {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    margin-bottom: 8px;
  }
  .approval-icon {
    flex-shrink: 0;
    margin-top: 2px;
  }
  .approval-info {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .approval-title {
    font-size: 13px;
    font-weight: 600;
    color: #303133;
  }
  .approval-tool {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: #606266;
  }
  .tool-name-inline {
    font-family: 'SFMono-Regular', Consolas, monospace;
    font-weight: 600;
  }
  .approval-args {
    margin: 8px 0;
    margin-left: 26px;
  }
  .approval-actions {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: 26px;
    margin-top: 10px;
  }
  .tool-section-label {
    font-size: 11px;
    font-weight: 600;
    color: #86909c;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }
  .tool-pre {
    margin: 0;
    padding: 10px 12px;
    background: #f7f8fa;
    border-radius: 6px;
    font-size: 12px;
    font-family: 'SFMono-Regular', Consolas, monospace;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    max-height: 320px;
    overflow-y: auto;
    color: #303133;
    line-height: 1.6;
  }
</style>
