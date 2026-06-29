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
  <div class="chat-message" :class="{ 'is-user': role === 'user' }">
    <template v-if="role === 'user'">
      <div class="bubble-wrapper">
        <div class="user-bubble">
          {{ text }}
        </div>
        <div class="bubble-actions">
          <button
            class="bubble-action-btn"
            :title="$t('data_agent.copy')"
            @click="copyText(text || '')">
            <el-icon :size="12"><DocumentCopy /></el-icon>
          </button>
        </div>
      </div>
      <div class="avatar avatar-user">
        <el-icon :size="16"><User /></el-icon>
      </div>
    </template>

    <template v-else>
      <div class="avatar avatar-assistant">
        <el-icon :size="16" color="#fff"><ChatDotRound /></el-icon>
      </div>
      <div class="bubble-wrapper">
        <div class="assistant-bubble">
          <div
            v-for="(block, idx) in blocks"
            :key="block.toolCallId ? `t-${block.toolCallId}` : `i-${idx}`"
            class="assistant-block">
            <ReasoningBlock
              v-if="block.type === 'reasoning'"
              :block="block"
              :thinking="
                !!streaming && idx === lastReasoningIdx && !hasContentAfter(idx)
              "
              @toggle="block.expanded = !block.expanded" />
            <TextBlock
              v-else-if="block.type === 'text'"
              :text="block.text || ''"
              :streaming="!!streaming && idx === lastTextBlockIdx" />
            <ApprovalBlock
              v-else-if="
                block.type === 'approval_request' &&
                block.approvalStatus === 'pending'
              "
              :block="block"
              :approving-request-id="approvingRequestId"
              @approve="(id) => emit('approve', id)"
              @deny="(id) => emit('deny', id)" />
            <ToolBlock
              v-else
              :block="block"
              @toggle="block.expanded = !block.expanded" />
          </div>

          <div v-if="streaming" class="streaming-indicator">
            <span class="streaming-dot"></span>
            <span class="streaming-label">{{
              $t('data_agent.generating')
            }}</span>
          </div>
        </div>
        <UsageFooter
          v-if="!streaming && usage && accumulatedTotal > 0"
          :usage="usage" />
        <div class="bubble-actions">
          <button
            class="bubble-action-btn"
            :title="$t('data_agent.copy')"
            @click="copyText(allAssistantText)">
            <el-icon :size="12"><DocumentCopy /></el-icon>
          </button>
        </div>
      </div>
    </template>
  </div>
</template>

<script lang="ts" setup>
  import { User, ChatDotRound, DocumentCopy } from '@element-plus/icons-vue'
  import { computed } from 'vue'
  import type { ChatBlock, TokenUsage } from '../types'
  import ReasoningBlock from '../blocks/ReasoningBlock.vue'
  import TextBlock from '../blocks/TextBlock.vue'
  import ApprovalBlock from '../blocks/ApprovalBlock.vue'
  import ToolBlock from '../blocks/ToolBlock.vue'
  import UsageFooter from '../blocks/UsageFooter.vue'
  import { useCopyText } from '../utils/clipboard'

  const props = defineProps<{
    role: 'user' | 'assistant'
    text?: string
    blocks?: ChatBlock[]
    streaming?: boolean
    approvingRequestId?: string
    usage?: TokenUsage
  }>()

  const emit = defineEmits<{
    (e: 'approve', requestId: string): void
    (e: 'deny', requestId: string): void
  }>()

  const copyText = useCopyText()

  const accumulatedTotal = computed(() =>
    props.usage
      ? props.usage.accumulatedPrompt + props.usage.accumulatedCompletion
      : 0
  )

  const allAssistantText = computed(() => {
    if (!props.blocks) return ''
    return props.blocks
      .filter((b) => b.type === 'text' && b.text)
      .map((b) => b.text)
      .join('\n\n')
  })

  const lastTextBlockIdx = computed(() => {
    if (!props.blocks) return -1
    for (let i = props.blocks.length - 1; i >= 0; i--) {
      if (props.blocks[i].type === 'text') return i
    }
    return -1
  })

  const lastReasoningIdx = computed(() => {
    if (!props.blocks) return -1
    for (let i = props.blocks.length - 1; i >= 0; i--) {
      if (props.blocks[i].type === 'reasoning') return i
    }
    return -1
  })

  function hasContentAfter(idx: number): boolean {
    if (!props.blocks) return false
    for (let i = idx + 1; i < props.blocks.length; i++) {
      const b = props.blocks[i]
      if (b.type === 'text' && b.text) return true
      if (b.type === 'tool_call' || b.type === 'approval_request') return true
    }
    return false
  }
</script>

<style lang="scss" scoped>
  .chat-message {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    margin-bottom: 20px;

    &.is-user {
      justify-content: flex-end;
    }
  }

  .avatar {
    flex-shrink: 0;
    width: 32px;
    height: 32px;
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-top: 2px;
  }
  .avatar-user {
    background: #e8f4ff;
    color: #409eff;
    order: 1;
  }
  .avatar-assistant {
    background: linear-gradient(135deg, #409eff, #3a7bd5);
  }

  .user-bubble {
    padding: 10px 16px;
    border-radius: 16px 16px 4px 16px;
    background: #409eff;
    color: #fff;
    font-size: 14px;
    line-height: 1.6;
    white-space: pre-wrap;
    word-break: break-word;
    box-shadow: 0 2px 8px rgba(64, 158, 255, 0.2);
  }

  .bubble-wrapper {
    display: flex;
    flex-direction: column;
    max-width: 85%;
    min-width: min(480px, 100%);
  }

  .is-user .bubble-wrapper {
    align-items: flex-end;
    max-width: 70%;
  }

  .bubble-actions {
    display: flex;
    gap: 4px;
    margin-top: 4px;
    opacity: 0;
    transition: opacity 0.15s;
  }

  .bubble-wrapper:hover .bubble-actions {
    opacity: 1;
  }

  .bubble-action-btn {
    width: 26px;
    height: 26px;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: #c0c4cc;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;

    &:hover {
      background: #f0f1f3;
      color: #606266;
    }
  }

  .assistant-bubble {
    min-width: min(480px, 100%);
    background: #fff;
    border: 1px solid #e5e6eb;
    border-radius: 4px 16px 16px 16px;
    overflow: hidden;
  }

  .assistant-block {
    &:not(:first-child) {
      border-top: 1px solid #f0f0f0;
    }
  }

  .streaming-indicator {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 16px;
    border-top: 1px solid #f0f0f0;
    background: #fafbfc;
  }

  .streaming-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #409eff;
    animation: pulse-stream 1.2s infinite ease-in-out;
  }

  @keyframes pulse-stream {
    0%,
    100% {
      opacity: 1;
      transform: scale(1);
    }
    50% {
      opacity: 0.4;
      transform: scale(0.75);
    }
  }

  .streaming-label {
    font-size: 12px;
    color: #909399;
    font-weight: 500;
  }
</style>
