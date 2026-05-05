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
  <!-- Used for both regular tool calls and resolved approval requests
       (the visual differs only in an extra status tag for approvals). -->
  <div class="tool-call-block">
    <div class="tool-header" @click="emit('toggle')">
      <div class="tool-header-left">
        <div class="tool-dot" :class="dotClass"></div>
        <span class="tool-name">{{ block.name }}</span>
        <el-tag
          v-if="approvalTagType"
          :type="approvalTagType"
          effect="plain"
          size="small">
          {{ approvalTagText }}
        </el-tag>
      </div>
      <div class="tool-header-right">
        <span class="tool-status" :class="dotClass">{{ statusText }}</span>
        <el-icon class="chevron" :class="{ 'is-expanded': block.expanded }">
          <ArrowDown />
        </el-icon>
      </div>
    </div>
    <Transition name="tool-expand">
      <div v-if="block.expanded" class="tool-body">
        <div v-if="block.args" class="tool-section">
          <div class="tool-section-label">{{ $t('data_agent.arguments') }}</div>
          <div class="tool-pre-wrapper">
            <pre class="tool-pre">{{ formatArgsForDisplay(block.args) }}</pre>
            <button
              class="copy-btn"
              :title="$t('data_agent.copy')"
              @click.stop="copyText(block.args || '')">
              <el-icon :size="12"><DocumentCopy /></el-icon>
            </button>
          </div>
        </div>
        <div v-if="block.result != null" class="tool-section">
          <div class="tool-section-label">{{ $t('data_agent.result') }}</div>
          <div v-if="block.isError" class="tool-pre-wrapper">
            <pre class="tool-pre is-error">{{ block.result }}</pre>
            <button
              class="copy-btn"
              :title="$t('data_agent.copy')"
              @click.stop="copyText(block.result || '')">
              <el-icon :size="12"><DocumentCopy /></el-icon>
            </button>
          </div>
          <div v-else class="tool-result-markdown">
            <MarkdownContent :text="block.result || ''" />
            <button
              class="copy-btn"
              :title="$t('data_agent.copy')"
              @click.stop="copyText(block.result || '')">
              <el-icon :size="12"><DocumentCopy /></el-icon>
            </button>
          </div>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script lang="ts" setup>
  import { computed } from 'vue'
  import { ArrowDown, DocumentCopy } from '@element-plus/icons-vue'
  import { useI18n } from 'vue-i18n'
  import type { ChatBlock } from '../types'
  import { formatArgsForDisplay } from '../utils/format'
  import { useCopyText } from '../utils/clipboard'
  import MarkdownContent from './MarkdownContent.vue'

  const props = defineProps<{
    block: ChatBlock
  }>()

  const emit = defineEmits<{ (e: 'toggle'): void }>()

  const { t } = useI18n()
  const copyText = useCopyText()

  const isApproval = computed(() => props.block.type === 'approval_request')
  const isDenied = computed(
    () => isApproval.value && props.block.approvalStatus === 'denied'
  )

  // Visual state: running / done / error.
  // Denied approvals render as error (red), regardless of `result`.
  const dotClass = computed(() => ({
    'is-running':
      props.block.result == null &&
      !isDenied.value &&
      (!isApproval.value || props.block.approvalStatus === 'approved'),
    'is-done': props.block.result != null && !props.block.isError,
    'is-error': props.block.isError || isDenied.value
  }))

  const statusText = computed(() => {
    if (isDenied.value) return t('data_agent.denied')
    if (props.block.result != null) {
      return props.block.isError ? t('data_agent.error') : t('data_agent.done')
    }
    return t('data_agent.running')
  })

  const approvalTagType = computed<'success' | 'danger' | null>(() => {
    if (!isApproval.value) return null
    return props.block.approvalStatus === 'approved' ? 'success' : 'danger'
  })

  const approvalTagText = computed(() => {
    if (!isApproval.value) return ''
    return props.block.approvalStatus === 'approved'
      ? t('data_agent.approved')
      : t('data_agent.denied')
  })
</script>

<style lang="scss" scoped>
  .tool-call-block {
    overflow: hidden;
    background: #fff;
  }

  .tool-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 14px;
    cursor: pointer;
    user-select: none;
    font-size: 13px;
    transition: background 0.15s;

    &:hover {
      background: #f7f8fa;
    }
  }
  .tool-header-left {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    flex: 1;
  }
  .tool-header-right {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-shrink: 0;
    margin-left: 16px;
  }

  .tool-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;

    &.is-running {
      background: #e6a23c;
      animation: pulse-dot 1.5s infinite;
    }
    &.is-done {
      background: #67c23a;
    }
    &.is-error {
      background: #f56c6c;
    }
  }

  @keyframes pulse-dot {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.4;
    }
  }

  .tool-name {
    font-weight: 600;
    color: #303133;
    font-family: 'SFMono-Regular', Consolas, monospace;
    font-size: 13px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .tool-status {
    font-size: 12px;
    font-weight: 500;

    &.is-running {
      color: #e6a23c;
    }
    &.is-done {
      color: #67c23a;
    }
    &.is-error {
      color: #f56c6c;
    }
  }

  .chevron {
    transition: transform 0.25s ease;
    color: #c0c4cc;

    &.is-expanded {
      transform: rotate(180deg);
    }
  }

  .tool-body {
    border-top: 1px solid #f0f0f0;
  }

  .tool-section {
    padding: 10px 14px;

    &:not(:last-child) {
      border-bottom: 1px solid #f5f5f5;
    }
  }

  .tool-section-label {
    font-size: 11px;
    font-weight: 600;
    color: #86909c;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }

  .tool-pre-wrapper {
    position: relative;

    .copy-btn {
      position: absolute;
      top: 6px;
      right: 6px;
      width: 24px;
      height: 24px;
      border: none;
      border-radius: 4px;
      background: rgba(0, 0, 0, 0.04);
      color: #86909c;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      opacity: 0;
      transition: all 0.15s;

      &:hover {
        background: rgba(0, 0, 0, 0.08);
        color: #4e5969;
      }
    }

    &:hover .copy-btn {
      opacity: 1;
    }
  }

  .tool-result-markdown {
    position: relative;
    padding: 8px 10px;
    background: #f7f8fa;
    border-radius: 6px;
    max-height: 320px;
    overflow: auto;
    font-size: 12px;
    line-height: 1.5;

    .markdown-body {
      :deep(p) {
        margin: 0 0 6px;
        font-size: 12px;
        &:last-child {
          margin-bottom: 0;
        }
      }
      :deep(table) {
        display: block;
        max-width: 100%;
        overflow-x: auto;
        border-collapse: collapse;
        margin: 4px 0;
        border-radius: 4px;
        font-size: 11px;

        &::-webkit-scrollbar {
          height: 6px;
        }
        &::-webkit-scrollbar-thumb {
          background: #dcdfe6;
          border-radius: 3px;
        }
      }
      :deep(th),
      :deep(td) {
        border: 1px solid #e5e6eb;
        padding: 4px 8px;
        text-align: left;
        font-family: 'SFMono-Regular', Consolas, monospace;
        font-size: 11px;
        white-space: nowrap;
      }
      :deep(th) {
        background: #eef0f4;
        font-weight: 600;
        color: #4e5969;
      }
      :deep(td) {
        color: #303133;
      }
    }

    .copy-btn {
      position: absolute;
      top: 6px;
      right: 6px;
      width: 24px;
      height: 24px;
      border: none;
      border-radius: 4px;
      background: rgba(0, 0, 0, 0.04);
      color: #86909c;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      opacity: 0;
      transition: all 0.15s;

      &:hover {
        background: rgba(0, 0, 0, 0.08);
        color: #4e5969;
      }
    }

    &:hover .copy-btn {
      opacity: 1;
    }
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

    &.is-error {
      background: #fff2f0;
      color: #f56c6c;
      border: 1px solid #ffccc7;
    }
  }

  .tool-expand-enter-active,
  .tool-expand-leave-active {
    transition: all 0.25s ease;
    overflow: hidden;
  }
  .tool-expand-enter-from,
  .tool-expand-leave-to {
    opacity: 0;
    max-height: 0;
  }
  .tool-expand-enter-to,
  .tool-expand-leave-from {
    max-height: 600px;
  }
</style>
