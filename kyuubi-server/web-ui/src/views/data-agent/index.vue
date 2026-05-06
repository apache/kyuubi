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
    <SessionsRail
      :sessions="sessions"
      :active-session-id="activeSessionId"
      @select="setActiveSession"
      @new="onNewSession"
      @close="onCloseSession" />
    <ChatPane
      ref="chatPaneRef"
      :active="active"
      :streaming="streaming"
      :initializing="initializing"
      :jdbc-url="jdbcUrlInput"
      :model-value="modelInput"
      :approval-mode="approvalModeInput"
      :display-jdbc-url="displayJdbcUrl"
      :datasource-label="datasourceLabel"
      :input-placeholder="inputPlaceholder"
      @update:jdbc-url="(v) => (jdbcUrlInput = v)"
      @update:model-value="(v) => (modelInput = v)"
      @update:approval-mode="(v) => (approvalModeInput = v)"
      @send="sendMessage"
      @approve="(id) => handleApproval(id, true)"
      @deny="(id) => handleApproval(id, false)"
      @stop="cancelActiveStream"
      @reset-from-error="handleResetFromError"
      @dismiss-error="dismissError" />
  </div>
</template>

<script lang="ts">
  export default { name: 'DataAgent' }
</script>

<script lang="ts" setup>
  import { ref, computed, onMounted } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { useDataAgentStore, type DataAgentSession } from '@/pinia/data-agent'
  import SessionsRail from './components/SessionsRail.vue'
  import ChatPane from './components/ChatPane.vue'
  import { sanitizeJdbcUrl } from './utils/jdbc'
  import { useStreamRegistry, useChatStream } from './composables/useChatStream'
  import {
    useSessionLifecycle,
    defaultApprovalMode,
    defaultModel,
    persistApprovalMode,
    persistModel
  } from './composables/useSessionLifecycle'
  import { useUrlSync } from './composables/useUrlSync'

  const { t } = useI18n()
  const store = useDataAgentStore()

  const chatPaneRef = ref<InstanceType<typeof ChatPane> | null>(null)

  const active = computed<DataAgentSession | undefined>(
    () => store.activeSession
  )
  const sessions = computed(() => store.orderedSessions)
  const activeSessionId = computed(() => store.activeSessionId)
  const streaming = computed(() => active.value?.streaming ?? false)
  const initializing = computed(() => active.value?.initializing ?? false)

  function patchActive(patch: Partial<DataAgentSession>) {
    const id = store.activeSessionId
    if (id) store.patchSession(id, patch)
  }

  const jdbcUrlInput = computed({
    get: () => active.value?.jdbcUrl ?? '',
    set: (v: string) => patchActive({ jdbcUrl: v })
  })
  const modelInput = computed({
    get: () => active.value?.model ?? '',
    set: (v: string) => {
      patchActive({ model: v })
      persistModel(v)
    }
  })
  const approvalModeInput = computed({
    get: () => active.value?.approvalMode ?? 'NORMAL',
    set: (v: string) => {
      patchActive({ approvalMode: v })
      persistApprovalMode(v)
    }
  })

  const displayJdbcUrl = computed(() =>
    sanitizeJdbcUrl((active.value?.jdbcUrl ?? '').trim())
  )

  const datasourceLabel = computed(() => {
    const url = (active.value?.jdbcUrl ?? '').trim()
    if (!url) return t('data_agent.server_default')
    try {
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

  const registry = useStreamRegistry()
  const { syncUrl, readUrlSessionId } = useUrlSync(store)
  const {
    ensureSession,
    dismissError,
    handleResetFromError,
    onNewSession,
    setActiveSession,
    onCloseSession
  } = useSessionLifecycle({ store, registry, syncUrl })

  const { sendMessage, handleApproval, cancelActiveStream } = useChatStream({
    store,
    registry,
    ensureSession,
    scrollToBottom: () => chatPaneRef.value?.scrollToBottom(),
    anchorToBottom: () => chatPaneRef.value?.anchorToBottom()
  })

  onMounted(() => {
    store.hydrateTransientFlags()

    const urlId = readUrlSessionId()

    if (urlId && store.sessions[urlId]) {
      store.setActive(urlId)
    } else if (!store.hasSessions) {
      store.createSession({
        approvalMode: defaultApprovalMode(),
        model: defaultModel()
      })
    } else if (
      !store.activeSessionId ||
      !store.sessions[store.activeSessionId]
    ) {
      store.setActive(store.sessionOrder[0])
    }

    syncUrl(store.activeSessionId)

    if ((active.value?.messages.length ?? 0) > 0) {
      chatPaneRef.value?.anchorToBottom()
    }
  })
</script>

<style lang="scss" scoped>
  .data-agent-page {
    display: flex;
    height: calc(100vh - 64px);
    margin: -20px;
    background: #f7f8fa;
    position: relative;
  }
</style>
