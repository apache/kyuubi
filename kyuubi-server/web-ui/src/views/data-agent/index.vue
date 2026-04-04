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
  <div class="data-agent-page">
    <!-- Header bar -->
    <div class="chat-header">
      <div class="header-left">
        <div class="header-logo">
          <el-icon :size="16" color="#fff"><ChatDotRound /></el-icon>
        </div>
        <span class="header-title">{{ $t('data_agent.title') }}</span>
        <el-tag
          v-if="sessionHandle"
          size="small"
          type="info"
          effect="plain"
          class="session-tag">
          {{ sessionHandle.substring(0, 8) }}
        </el-tag>
        <el-tag
          v-if="sessionHandle && datasourceLabel"
          size="small"
          effect="plain"
          class="datasource-tag"
          :title="jdbcUrl || $t('data_agent.server_default')">
          <el-icon :size="12"><Link /></el-icon>
          {{ datasourceLabel }}
        </el-tag>
      </div>
      <div class="header-right">
        <el-tooltip
          :content="$t('data_agent.approval_tooltip')"
          placement="bottom"
          :show-after="500">
          <el-select
            v-model="approvalMode"
            size="small"
            style="width: 160px"
            :disabled="streaming"
            @change="onApprovalModeChange">
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
          @click="cancelStream">
          {{ $t('data_agent.stop') }}
        </el-button>
        <el-button
          v-if="sessionHandle || messages.length > 0"
          size="small"
          plain
          :icon="RefreshRight"
          @click="resetSession">
          {{ $t('data_agent.new_chat') }}
        </el-button>
      </div>
    </div>

    <!-- Messages area -->
    <div ref="messagesContainer" class="messages-area">
      <!-- Welcome state -->
      <div v-if="messages.length === 0 && !streaming" class="welcome">
        <div class="welcome-hero">
          <div class="welcome-icon">
            <el-icon :size="36" color="#fff"><ChatDotRound /></el-icon>
          </div>
          <h2>{{ $t('data_agent.title') }}</h2>
          <p class="welcome-desc">
            {{ $t('data_agent.welcome_desc') }}
          </p>
        </div>

        <!-- Config & quick start card -->
        <div class="config-card">
          <div class="config-card-header">
            <el-icon :size="14"><Setting /></el-icon>
            <span>{{ $t('data_agent.connection') }}</span>
          </div>
          <div class="config-card-body">
            <div class="ds-field ds-field-grow">
              <label>{{ $t('data_agent.jdbc_url') }}</label>
              <el-tooltip
                :disabled="!sessionHandle"
                :content="$t('data_agent.change_jdbc')"
                placement="top">
                <el-autocomplete
                  v-model="jdbcUrl"
                  :disabled="!!sessionHandle"
                  :placeholder="$t('data_agent.jdbc_placeholder')"
                  :fetch-suggestions="queryJdbcSuggestions"
                  :trigger-on-focus="true"
                  size="default"
                  clearable
                  style="width: 100%">
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
                @click="sendMessage(q.text)">
                <el-icon :size="14"><component :is="q.icon" /></el-icon>
                <span>{{ q.text }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Message list -->
      <TransitionGroup name="msg-fade">
        <ChatMessage
          v-for="msg in messages"
          v-show="
            msg.role === 'user' ||
            (msg.blocks && msg.blocks.length > 0) ||
            (streaming && msg.id === messages[messages.length - 1]?.id)
          "
          :key="msg.id"
          :role="msg.role"
          :text="msg.text"
          :blocks="msg.blocks"
          :streaming="streaming && msg.id === messages[messages.length - 1]?.id"
          :approving-request-id="approvingRequestId"
          @approve="(id: string) => handleApproval(id, true)"
          @deny="(id: string) => handleApproval(id, false)" />
      </TransitionGroup>

      <!-- Error display -->
      <Transition name="fade">
        <div v-if="errorMessage" class="error-banner">
          <el-icon :size="16"><WarningFilled /></el-icon>
          <span>{{ errorMessage }}</span>
          <el-button
            text
            size="small"
            type="danger"
            :icon="Close"
            circle
            @click="errorMessage = ''" />
        </div>
      </Transition>
    </div>

    <!-- Input bar -->
    <InputBar
      :disabled="streaming || initializing"
      :placeholder="inputPlaceholder"
      @send="sendMessage" />

    <!-- Initializing pill -->
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
  import { ref, computed, watch, onBeforeUnmount, onMounted } from 'vue'
  import { useI18n } from 'vue-i18n'
  import {
    ChatDotRound,
    ChatLineSquare,
    RefreshRight,
    Lock,
    WarningFilled,
    Close,
    Loading,
    Setting,
    VideoPause,
    Grid,
    Search,
    DataAnalysis,
    Link
  } from '@element-plus/icons-vue'
  import { ElMessage } from 'element-plus'
  import ChatMessage from './components/ChatMessage.vue'
  import type { ChatBlock } from './components/ChatMessage.vue'
  import InputBar from './components/InputBar.vue'
  import {
    openSession,
    closeSession,
    getSession,
    chatStream,
    approveToolCall
  } from '@/api/data-agent'

  const { t } = useI18n()

  interface Message {
    id: number
    role: 'user' | 'assistant'
    text?: string
    blocks?: ChatBlock[]
  }

  const jdbcTemplates = [
    {
      label: 'Spark / Hive (Thrift)',
      value:
        'jdbc:hive2://localhost:10009/default;user=username;password=password',
      isHistory: false
    },
    {
      label: 'Trino',
      value:
        'jdbc:trino://localhost:8080/catalog/schema?user=username&password=password',
      isHistory: false
    },
    {
      label: 'MySQL',
      value:
        'jdbc:mysql://localhost:3306/mydb?user=username&password=password&useSSL=false',
      isHistory: false
    }
  ]

  const JDBC_HISTORY_KEY = 'data-agent-jdbc-history'

  function loadJdbcHistory(): string[] {
    try {
      return JSON.parse(localStorage.getItem(JDBC_HISTORY_KEY) || '[]')
    } catch {
      return []
    }
  }

  function saveJdbcToHistory(url: string) {
    if (!url.trim()) return
    const history = loadJdbcHistory().filter((u) => u !== url)
    history.unshift(url)
    if (history.length > 10) history.length = 10
    localStorage.setItem(JDBC_HISTORY_KEY, JSON.stringify(history))
  }

  function removeJdbcFromHistory(url: string) {
    const history = loadJdbcHistory().filter((u) => u !== url)
    localStorage.setItem(JDBC_HISTORY_KEY, JSON.stringify(history))
  }

  interface JdbcSuggestion {
    label: string
    value: string
    isHistory: boolean
  }

  function queryJdbcSuggestions(
    query: string,
    cb: (results: JdbcSuggestion[]) => void
  ) {
    const history = loadJdbcHistory()
    const templateValues = new Set(jdbcTemplates.map((t) => t.value))
    const historyItems: JdbcSuggestion[] = history
      .filter((u) => !templateValues.has(u))
      .map((u) => ({
        label: t('data_agent.history'),
        value: u,
        isHistory: true
      }))
    const all = [...historyItems, ...jdbcTemplates]
    if (!query) {
      cb(all)
    } else {
      const q = query.toLowerCase()
      cb(
        all.filter(
          (item) =>
            item.value.toLowerCase().includes(q) ||
            item.label.toLowerCase().includes(q)
        )
      )
    }
  }

  const quickQuestions = computed(() => [
    { text: t('data_agent.quick_tables'), icon: Grid },
    { text: t('data_agent.quick_schema'), icon: Search },
    { text: t('data_agent.quick_records'), icon: DataAnalysis }
  ])

  const STORAGE_KEY = 'data-agent-state'

  interface StoredState {
    sessionHandle: string
    messages: Message[]
    msgIdCounter: number
    jdbcUrl: string
    approvalMode: string
  }

  function saveState() {
    const state: StoredState = {
      sessionHandle: sessionHandle.value,
      messages: messages.value,
      msgIdCounter,
      jdbcUrl: jdbcUrl.value,
      approvalMode: approvalMode.value
    }
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))
  }

  function restoreState() {
    const raw = sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return
    try {
      const state: StoredState = JSON.parse(raw)
      sessionHandle.value = state.sessionHandle || ''
      messages.value = state.messages || []
      msgIdCounter = state.msgIdCounter || 0
      jdbcUrl.value = state.jdbcUrl || ''
      approvalMode.value = state.approvalMode || 'NORMAL'
    } catch {
      sessionStorage.removeItem(STORAGE_KEY)
    }
  }

  function clearState() {
    sessionStorage.removeItem(STORAGE_KEY)
  }

  const jdbcUrl = ref('')
  const APPROVAL_MODE_KEY = 'data-agent-approval-mode'
  const approvalMode = ref(localStorage.getItem(APPROVAL_MODE_KEY) || 'NORMAL')

  function onApprovalModeChange(val: string) {
    localStorage.setItem(APPROVAL_MODE_KEY, val)
  }

  const messagesContainer = ref<HTMLDivElement>()
  const messages = ref<Message[]>([])
  const sessionHandle = ref('')
  const streaming = ref(false)
  const errorMessage = ref('')
  const initializing = ref(false)
  const approvingRequestId = ref('')
  let msgIdCounter = 0
  let abortController: AbortController | null = null
  let isUnmounted = false

  const datasourceLabel = computed(() => {
    const url = jdbcUrl.value.trim()
    if (!url) return t('data_agent.server_default')
    // Extract meaningful part from JDBC URL: show host/database or engine type
    try {
      // e.g. jdbc:hive2://host:port/db#... → host:port/db
      const afterProtocol = url.replace(/^jdbc:\w+:\/\//, '')
      const beforeHash = afterProtocol.split('#')[0].split('?')[0]
      if (beforeHash && beforeHash.length <= 40) return beforeHash
      return beforeHash.substring(0, 37) + '...'
    } catch {
      return url.substring(0, 30)
    }
  })

  const inputPlaceholder = computed(() => {
    if (initializing.value) return t('data_agent.starting_engine')
    if (streaming.value) return t('data_agent.waiting_response')
    return t('data_agent.input_placeholder')
  })

  async function ensureSession(): Promise<boolean> {
    if (sessionHandle.value) {
      try {
        await getSession(sessionHandle.value)
        return true
      } catch {
        sessionHandle.value = ''
        ElMessage.warning(t('data_agent.session_expired'))
        return false
      }
    }
    initializing.value = true
    try {
      const configs: Record<string, string> = {
        'kyuubi.engine.type': 'DATA_AGENT'
      }
      if (jdbcUrl.value.trim()) {
        configs['kyuubi.engine.data.agent.jdbc.url'] = jdbcUrl.value.trim()
      }
      const res: any = await openSession({ configs })
      sessionHandle.value = res.identifier || res.id || ''
      if (!sessionHandle.value) throw new Error('No session handle returned')
      saveJdbcToHistory(jdbcUrl.value.trim())
      return true
    } catch (e: any) {
      ElMessage.error(
        t('data_agent.session_start_failed', { message: e.message })
      )
      return false
    } finally {
      initializing.value = false
    }
  }

  async function resetSession() {
    if (streaming.value) cancelStream()
    if (sessionHandle.value) {
      try {
        await closeSession(sessionHandle.value)
      } catch (e: any) {
        console.warn('Failed to close session:', e.message) // eslint-disable-line no-console
        ElMessage.warning(t('data_agent.session_close_failed'))
      }
    }
    sessionHandle.value = ''
    messages.value = []
    errorMessage.value = ''
    clearState()
  }

  async function sendMessage(text: string) {
    if (streaming.value || initializing.value) return
    if (!(await ensureSession())) return

    messages.value.push({ id: ++msgIdCounter, role: 'user', text })
    messages.value.push({ id: ++msgIdCounter, role: 'assistant', blocks: [] })
    const assistantMsg = messages.value[messages.value.length - 1]

    scrollToBottom()
    await startStreaming(assistantMsg, text)
  }

  async function startStreaming(assistantMsg: Message, text: string) {
    streaming.value = true
    errorMessage.value = ''
    abortController = new AbortController()

    try {
      await chatStream(
        sessionHandle.value,
        text,
        (event) => handleSseEvent(assistantMsg, event),
        abortController.signal,
        approvalMode.value
      )
    } catch (e: any) {
      if (e.name !== 'AbortError') {
        errorMessage.value = e.message || t('data_agent.stream_error')
      }
    } finally {
      if (!isUnmounted) {
        streaming.value = false
        abortController = null
        debouncedSave()
      }
    }
  }

  function handleSseEvent(
    msg: Message,
    event: { event: string; data: string }
  ) {
    if (isUnmounted) return
    const blocks = msg.blocks!
    let parsed: any
    try {
      parsed = JSON.parse(event.data)
    } catch {
      console.warn('Invalid SSE event data:', event.data) // eslint-disable-line no-console
      errorMessage.value = t('data_agent.malformed_response')
      return
    }

    switch (event.event) {
      case 'content_delta': {
        const text = parsed.text || ''
        if (!text) break
        const last = blocks[blocks.length - 1]
        if (last && last.type === 'text') {
          last.text = (last.text || '') + text
        } else {
          blocks.push({ type: 'text', text })
        }
        break
      }
      case 'tool_call': {
        const toolCallId = parsed.id
        if (!toolCallId || !parsed.name) break
        // Skip if an approval_request block already exists for this tool call
        const hasApproval = blocks.some(
          (b) => b.type === 'approval_request' && b.toolCallId === toolCallId
        )
        if (!hasApproval) {
          blocks.push({
            type: 'tool_call',
            toolCallId,
            name: parsed.name,
            args: parsed.args,
            expanded: false
          })
        }
        break
      }
      case 'tool_result':
        for (let i = blocks.length - 1; i >= 0; i--) {
          const b = blocks[i]
          if (
            (b.type === 'tool_call' || b.type === 'approval_request') &&
            b.toolCallId === parsed.id
          ) {
            b.result = parsed.output
            b.isError = !!parsed.isError
            break
          }
        }
        break
      case 'approval_request':
        if (!parsed.requestId || !parsed.id || !parsed.name) break
        blocks.push({
          type: 'approval_request',
          toolCallId: parsed.id,
          name: parsed.name,
          args: parsed.args,
          requestId: parsed.requestId,
          riskLevel: parsed.riskLevel,
          approvalStatus: 'pending'
        })
        break
      case 'error':
        errorMessage.value = parsed.message || t('data_agent.unknown_error')
        break
      case 'done':
        break
    }
    scrollToBottom()
  }

  async function handleApproval(requestId: string, approved: boolean) {
    if (!sessionHandle.value || approvingRequestId.value) return
    approvingRequestId.value = requestId
    // Update block status immediately for responsiveness
    for (const msg of messages.value) {
      if (!msg.blocks) continue
      for (const block of msg.blocks) {
        if (
          block.type === 'approval_request' &&
          block.requestId === requestId
        ) {
          if (block.approvalStatus !== 'pending') {
            approvingRequestId.value = ''
            return // prevent double-click
          }
          block.approvalStatus = approved ? 'approved' : 'denied'
        }
      }
    }
    try {
      await approveToolCall(sessionHandle.value, requestId, approved)
    } catch (e: any) {
      errorMessage.value = t('data_agent.approval_failed', {
        message: e.message
      })
      // Revert status so user can retry
      for (const msg of messages.value) {
        if (!msg.blocks) continue
        for (const block of msg.blocks) {
          if (
            block.type === 'approval_request' &&
            block.requestId === requestId
          ) {
            block.approvalStatus = 'pending'
          }
        }
      }
    } finally {
      approvingRequestId.value = ''
    }
  }

  function cancelStream() {
    abortController?.abort()
  }

  let scrollRafId: number | null = null
  function scrollToBottom() {
    if (scrollRafId) return // throttle: at most once per animation frame
    scrollRafId = requestAnimationFrame(() => {
      scrollRafId = null
      const el = messagesContainer.value
      if (!el) return
      // Only auto-scroll if user is near the bottom (within 150px)
      const distanceFromBottom =
        el.scrollHeight - el.scrollTop - el.clientHeight
      if (distanceFromBottom < 150) {
        el.scrollTop = el.scrollHeight
      }
    })
  }

  let saveTimer: ReturnType<typeof setTimeout> | null = null
  function debouncedSave() {
    if (saveTimer) clearTimeout(saveTimer)
    saveTimer = setTimeout(saveState, 500)
  }

  watch([sessionHandle, jdbcUrl, approvalMode], () => debouncedSave())

  onMounted(() => {
    restoreState()
    if (messages.value.length > 0) {
      scrollToBottom()
    }
  })

  onBeforeUnmount(() => {
    isUnmounted = true
    cancelStream()
    if (saveTimer) clearTimeout(saveTimer)
    if (scrollRafId) cancelAnimationFrame(scrollRafId)
  })
</script>

<style lang="scss" scoped>
  .data-agent-page {
    display: flex;
    flex-direction: column;
    height: calc(100vh - 64px);
    margin: -20px;
    background: #f7f8fa;
    position: relative;
  }

  // Header
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

  // Messages
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

  // Welcome state
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

  // Config card
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

  // JDBC template dropdown options
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

  // Card section header
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

  // Error
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

  // Init pill
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

  // Transitions
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
