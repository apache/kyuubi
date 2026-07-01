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
  <div class="data-agent-main">
    <div class="chat-header">
      <div class="header-left">
        <div class="header-logo">
          <el-icon :size="16" color="#fff"><ChatDotRound /></el-icon>
        </div>
        <span class="header-title">{{ $t('data_agent.title') }}</span>
        <el-tooltip
          v-if="active && active.sessionHandle"
          :content="
            $t('data_agent.session_tooltip', { id: active.sessionHandle })
          "
          placement="bottom"
          :show-after="300">
          <el-tag size="small" type="info" effect="plain" class="session-tag">
            <span class="tag-label">{{ $t('data_agent.session_label') }}</span>
            {{ shortHandle(active.sessionHandle) }}
          </el-tag>
        </el-tooltip>
        <el-tooltip
          v-if="active && active.sessionHandle && datasourceLabel"
          :content="
            displayJdbcUrl || $t('data_agent.datasource_tooltip_default')
          "
          placement="bottom"
          :show-after="300">
          <el-tag size="small" effect="plain" class="datasource-tag">
            <el-icon :size="12"><Link /></el-icon>
            <span class="tag-label">{{
              $t('data_agent.datasource_label')
            }}</span>
            {{ datasourceLabel }}
          </el-tag>
        </el-tooltip>
      </div>
      <div class="header-right">
        <el-tooltip
          :content="$t('data_agent.model_tooltip')"
          placement="bottom"
          :show-after="500">
          <el-input
            :model-value="modelValue"
            size="small"
            style="width: 180px"
            :disabled="streaming"
            :placeholder="$t('data_agent.model_placeholder')"
            clearable
            @update:model-value="(v: string) => emit('update:modelValue', v)">
            <template #prefix>
              <el-icon :size="14"><Cpu /></el-icon>
            </template>
          </el-input>
        </el-tooltip>
        <el-tooltip
          :content="$t('data_agent.approval_tooltip')"
          placement="bottom"
          :show-after="500">
          <el-select
            :model-value="approvalMode"
            size="small"
            style="width: 160px"
            :disabled="streaming"
            @update:model-value="(v: string) => emit('update:approvalMode', v)">
            <template #prefix>
              <el-icon :size="14"><Lock /></el-icon>
            </template>
            <el-option
              :label="$t('data_agent.auto_approve')"
              value="AUTO_APPROVE" />
            <el-option :label="$t('data_agent.normal')" value="NORMAL" />
            <el-option :label="$t('data_agent.strict')" value="STRICT" />
          </el-select>
        </el-tooltip>
        <el-button
          v-if="streaming"
          size="small"
          type="danger"
          plain
          :icon="VideoPause"
          @click="emit('stop')">
          {{ $t('data_agent.stop') }}
        </el-button>
      </div>
    </div>

    <div
      ref="messagesContainer"
      class="messages-area"
      @scroll.passive="onMessagesScroll">
      <div
        v-if="active && active.messages.length === 0 && !streaming"
        class="welcome">
        <div class="welcome-hero">
          <div class="welcome-icon">
            <el-icon :size="36" color="#fff"><ChatDotRound /></el-icon>
          </div>
          <h2>{{ $t('data_agent.title') }}</h2>
          <p class="welcome-desc">
            {{ $t('data_agent.welcome_desc') }}
          </p>
        </div>

        <div class="config-card">
          <div class="config-card-header">
            <el-icon :size="14"><Setting /></el-icon>
            <span>{{ $t('data_agent.connection') }}</span>
          </div>
          <div class="config-card-body">
            <div class="ds-field ds-field-grow">
              <label>{{ $t('data_agent.jdbc_url') }}</label>
              <el-tooltip
                :disabled="!active || !active.sessionHandle"
                :content="$t('data_agent.change_jdbc')"
                placement="top">
                <el-autocomplete
                  :model-value="jdbcUrl"
                  :disabled="!!active?.sessionHandle"
                  :placeholder="$t('data_agent.jdbc_placeholder')"
                  :fetch-suggestions="queryJdbcSuggestions"
                  :trigger-on-focus="true"
                  size="default"
                  clearable
                  style="width: 100%"
                  @update:model-value="
                    (v: string) => emit('update:jdbcUrl', v)
                  ">
                  <template #default="{ item }">
                    <div class="jdbc-option">
                      <div class="jdbc-option-text">
                        <span class="jdbc-option-label">{{ item.label }}</span>
                        <span class="jdbc-option-url">{{ item.value }}</span>
                      </div>
                      <button
                        v-if="item.isHistory"
                        class="jdbc-option-del"
                        :title="$t('operation.delete')"
                        @click.stop="removeJdbcFromHistory(item.value)">
                        <el-icon :size="12"><Close /></el-icon>
                      </button>
                    </div>
                  </template>
                </el-autocomplete>
              </el-tooltip>
            </div>
          </div>
          <div class="config-card-section">
            <el-icon :size="14"><ChatLineSquare /></el-icon>
            <span>{{ $t('data_agent.try_asking') }}</span>
          </div>
          <div class="config-card-body">
            <div class="quick-chips">
              <div
                v-for="q in quickQuestions"
                :key="q.text"
                class="quick-chip"
                @click="emit('send', q.text)">
                <el-icon :size="14"><component :is="q.icon" /></el-icon>
                <span>{{ q.text }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <TransitionGroup v-if="active" name="msg-fade">
        <ChatMessage
          v-for="msg in active.messages"
          v-show="
            msg.role === 'user' ||
            (msg.blocks && msg.blocks.length > 0) ||
            (streaming &&
              msg.id === active.messages[active.messages.length - 1]?.id)
          "
          :key="msg.id"
          :role="msg.role"
          :text="msg.text"
          :blocks="msg.blocks"
          :usage="msg.usage"
          :streaming="
            streaming &&
            msg.id === active.messages[active.messages.length - 1]?.id
          "
          :approving-request-id="active.approvingRequestId"
          @approve="(id: string) => emit('approve', id)"
          @deny="(id: string) => emit('deny', id)" />
      </TransitionGroup>

      <Transition name="fade">
        <div v-if="active && active.errorMessage" class="error-banner">
          <el-icon :size="16"><WarningFilled /></el-icon>
          <span>{{ active.errorMessage }}</span>
          <el-button
            v-if="active.errorCanReset"
            size="small"
            type="danger"
            @click="emit('reset-from-error')">
            {{ $t('data_agent.reset_session') }}
          </el-button>
          <el-button
            text
            size="small"
            type="danger"
            :icon="Close"
            circle
            @click="emit('dismiss-error')" />
        </div>
      </Transition>
    </div>

    <InputBar
      :disabled="streaming || initializing"
      :placeholder="inputPlaceholder"
      @send="(text: string) => emit('send', text)" />

    <Transition name="slide-up">
      <div v-if="initializing" class="init-pill">
        <el-icon class="is-loading" :size="14" color="#409eff"
          ><Loading
        /></el-icon>
        <span>{{ $t('data_agent.starting_engine') }}</span>
      </div>
    </Transition>
  </div>
</template>

<script lang="ts" setup>
  import { computed, watch } from 'vue'
  import { useI18n } from 'vue-i18n'
  import {
    ChatDotRound,
    ChatLineSquare,
    Lock,
    WarningFilled,
    Close,
    Loading,
    Setting,
    VideoPause,
    Grid,
    Search,
    DataAnalysis,
    Link,
    Cpu
  } from '@element-plus/icons-vue'
  import ChatMessage from './ChatMessage.vue'
  import InputBar from './InputBar.vue'
  import type { DataAgentSession } from '@/pinia/data-agent'
  import {
    buildJdbcSuggestions,
    removeJdbcFromHistory,
    type JdbcSuggestion
  } from '../utils/jdbc'
  import { useChatScroll } from '../composables/useChatScroll'

  const props = defineProps<{
    active: DataAgentSession | undefined
    streaming: boolean
    initializing: boolean
    jdbcUrl: string
    modelValue: string // bound to model name
    approvalMode: string
    displayJdbcUrl: string
    datasourceLabel: string
    inputPlaceholder: string
  }>()

  const emit = defineEmits<{
    (e: 'update:jdbcUrl', value: string): void
    (e: 'update:modelValue', value: string): void
    (e: 'update:approvalMode', value: string): void
    (e: 'send', text: string): void
    (e: 'approve', requestId: string): void
    (e: 'deny', requestId: string): void
    (e: 'stop'): void
    (e: 'reset-from-error'): void
    (e: 'dismiss-error'): void
  }>()

  const { t } = useI18n()
  const {
    messagesContainer,
    scrollToBottom,
    onMessagesScroll,
    anchorToBottom
  } = useChatScroll()

  const quickQuestions = computed(() => [
    { text: t('data_agent.quick_tables'), icon: Grid },
    { text: t('data_agent.quick_schema'), icon: Search },
    { text: t('data_agent.quick_records'), icon: DataAnalysis }
  ])

  function shortHandle(handle: string): string {
    return handle ? handle.substring(0, 8) : ''
  }

  function queryJdbcSuggestions(
    query: string,
    cb: (results: JdbcSuggestion[]) => void
  ) {
    cb(buildJdbcSuggestions(query, t('data_agent.history')))
  }

  watch(
    () => props.active?.id,
    () => {
      anchorToBottom()
    }
  )

  defineExpose({ scrollToBottom, anchorToBottom })
</script>

<style lang="scss" scoped>
  .data-agent-main {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
    order: 0;
    position: relative;
  }

  .chat-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 52px;
    padding: 0 24px;
    background: #fff;
    border-bottom: 1px solid #ebeef5;
    flex-shrink: 0;
  }
  .header-left {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .header-logo {
    width: 28px;
    height: 28px;
    border-radius: 8px;
    background: linear-gradient(135deg, #409eff, #3a7bd5);
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .header-title {
    font-size: 15px;
    font-weight: 600;
    color: #303133;
  }
  .session-tag {
    font-family: 'SFMono-Regular', Consolas, monospace;
    font-size: 11px;
  }
  .tag-label {
    margin-right: 4px;
    font-family:
      -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    color: #909399;
    font-weight: 500;
  }
  .datasource-tag {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: default;
  }
  .header-right {
    display: flex;
    gap: 8px;
  }

  .messages-area {
    flex: 1;
    overflow-y: auto;
    padding: 24px 10%;
    scroll-behavior: smooth;

    &::-webkit-scrollbar {
      width: 6px;
    }
    &::-webkit-scrollbar-thumb {
      background: #c0c4cc;
      border-radius: 3px;
    }
    &::-webkit-scrollbar-track {
      background: transparent;
    }
  }

  .welcome {
    display: flex;
    flex-direction: column;
    align-items: center;
    padding-top: 6vh;
  }
  .welcome-hero {
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
    margin-bottom: 32px;

    h2 {
      margin: 16px 0 8px;
      font-size: 24px;
      font-weight: 700;
      color: #1d2129;
    }
    .welcome-desc {
      max-width: 560px;
      margin: 0;
      font-size: 14px;
      color: #86909c;
      line-height: 1.7;
    }
  }
  .welcome-icon {
    width: 72px;
    height: 72px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 20px;
    background: linear-gradient(135deg, #409eff, #3a7bd5);
    box-shadow: 0 8px 24px rgba(64, 158, 255, 0.3);
  }

  .config-card {
    width: 100%;
    max-width: 640px;
    background: #fff;
    border-radius: 12px;
    border: 1px solid #e5e6eb;
    margin-bottom: 28px;
    overflow: hidden;
  }
  .config-card-header {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 600;
    color: #606266;
    background: #fafafa;
    border-bottom: 1px solid #f0f0f0;
  }
  .config-card-body {
    padding: 16px;
  }
  .ds-field {
    display: flex;
    flex-direction: column;
    gap: 6px;

    label {
      font-size: 12px;
      font-weight: 600;
      color: #86909c;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
  }
  .ds-field-grow {
    flex: 1;
    min-width: 0;
  }

  .jdbc-option {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 2px 0;
    line-height: 1.4;
  }
  .jdbc-option-text {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
  }
  .jdbc-option-label {
    font-size: 13px;
    font-weight: 500;
    color: #303133;
  }
  .jdbc-option-url {
    font-size: 11px;
    font-family: 'SFMono-Regular', Consolas, monospace;
    color: #909399;
  }
  .jdbc-option-del {
    width: 20px;
    height: 20px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: #c0c4cc;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
    flex-shrink: 0;

    &:hover {
      background: #fee;
      color: #f56c6c;
    }
  }

  .config-card-section {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 600;
    color: #606266;
    background: #fafafa;
    border-top: 1px solid #f0f0f0;
  }
  .quick-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .quick-chip {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 5px 12px;
    background: #fff;
    border: 1px solid #e5e6eb;
    border-radius: 16px;
    font-size: 12px;
    color: #4e5969;
    cursor: pointer;
    transition: all 0.2s ease;

    &:hover {
      border-color: #409eff;
      color: #409eff;
      background: #f0f7ff;
    }
  }

  .error-banner {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 12px 10px 14px;
    border-radius: 10px;
    background: #fff2f0;
    border: 1px solid #ffccc7;
    color: #f56c6c;
    font-size: 13px;
    margin-top: 12px;
  }

  .init-pill {
    position: absolute;
    bottom: 84px;
    left: 50%;
    transform: translateX(-50%);
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 20px;
    background: #fff;
    border: 1px solid #e5e6eb;
    border-radius: 24px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.08);
    font-size: 13px;
    color: #4e5969;
    z-index: 10;
  }

  .fade-enter-active,
  .fade-leave-active {
    transition: opacity 0.3s ease;
  }
  .fade-enter-from,
  .fade-leave-to {
    opacity: 0;
  }

  .msg-fade-enter-active {
    transition: all 0.3s ease;
  }
  .msg-fade-enter-from {
    opacity: 0;
    transform: translateY(12px);
  }

  .slide-up-enter-active,
  .slide-up-leave-active {
    transition: all 0.3s ease;
  }
  .slide-up-enter-from,
  .slide-up-leave-to {
    opacity: 0;
    transform: translateX(-50%) translateY(8px);
  }
</style>
