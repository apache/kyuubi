/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { openSession, closeSession, getSession } from '@/api/data-agent'
import { isValidApprovalMode, useDataAgentStore } from '@/pinia/data-agent'
import { sanitizeJdbcUrl, saveJdbcToHistory } from '../utils/jdbc'
import type { StreamRegistry } from './useChatStream'

const APPROVAL_MODE_DEFAULT_KEY = 'data-agent-approval-mode'
const MODEL_DEFAULT_KEY = 'data-agent-model'

export function defaultApprovalMode(): string {
  const v = localStorage.getItem(APPROVAL_MODE_DEFAULT_KEY)
  return isValidApprovalMode(v) ? (v as string) : 'NORMAL'
}

export function defaultModel(): string {
  return localStorage.getItem(MODEL_DEFAULT_KEY) || ''
}

export function persistApprovalMode(mode: string) {
  try {
    localStorage.setItem(APPROVAL_MODE_DEFAULT_KEY, mode)
  } catch {
    /* ignore */
  }
}

export function persistModel(model: string) {
  try {
    localStorage.setItem(MODEL_DEFAULT_KEY, (model || '').trim())
  } catch {
    /* ignore */
  }
}

export function useSessionLifecycle(opts: {
  store: ReturnType<typeof useDataAgentStore>
  registry: StreamRegistry
  syncUrl: (id: string | null) => void
}) {
  const { store, registry, syncUrl } = opts
  const { t } = useI18n()

  async function ensureSession(id: string): Promise<boolean> {
    const s = store.sessions[id]
    if (!s) return false
    if (s.sessionHandle) {
      try {
        await getSession(s.sessionHandle)
        return true
      } catch {
        store.patchSession(id, { sessionHandle: '' })
        ElMessage.warning(t('data_agent.session_expired'))
        return false
      }
    }
    store.patchSession(id, { initializing: true })
    try {
      const configs: Record<string, string> = {
        'kyuubi.engine.type': 'DATA_AGENT'
      }
      const rawJdbc = s.jdbcUrl.trim()
      if (rawJdbc) {
        configs['kyuubi.engine.data.agent.jdbc.url'] = rawJdbc
      }
      const res: any = await openSession({ configs })
      const handle = res.identifier || res.id || ''
      if (!handle) throw new Error('No session handle returned')
      store.patchSession(id, { sessionHandle: handle })
      saveJdbcToHistory(rawJdbc)
      // Replace raw URL with sanitized form so credentials don't survive in
      // sessionStorage or the input.
      store.patchSession(id, { jdbcUrl: sanitizeJdbcUrl(rawJdbc) })
      return true
    } catch (e: any) {
      ElMessage.error(
        t('data_agent.session_start_failed', { message: e.message })
      )
      return false
    } finally {
      store.patchSession(id, { initializing: false })
    }
  }

  async function resetActiveSession() {
    const id = store.activeSessionId
    if (!id) return
    const s = store.sessions[id]
    if (!s) return
    if (s.streaming) registry.cancelStream(id)
    const handle = s.sessionHandle
    if (handle) {
      try {
        await closeSession(handle)
      } catch (e: any) {
        // eslint-disable-next-line no-console
        console.warn('Failed to close session:', e.message)
        ElMessage.warning(t('data_agent.session_close_failed'))
      }
    }
    store.resetSession(id)
  }

  function dismissError() {
    const id = store.activeSessionId
    if (id) store.patchSession(id, { errorMessage: '', errorCanReset: false })
  }

  async function handleResetFromError() {
    dismissError()
    await resetActiveSession()
  }

  function onNewSession() {
    const id = store.createSession({
      approvalMode: defaultApprovalMode(),
      model: defaultModel()
    })
    syncUrl(id)
  }

  function setActiveSession(id: string) {
    store.setActive(id)
    syncUrl(id)
  }

  async function onCloseSession(id: string) {
    const s = store.sessions[id]
    if (!s) return
    if (s.streaming) registry.cancelStream(id)
    const handle = s.sessionHandle
    // Best-effort backend cleanup; failures shouldn't block UI removal.
    if (handle) {
      try {
        await closeSession(handle)
      } catch (e: any) {
        // eslint-disable-next-line no-console
        console.warn('Failed to close session:', e.message)
      }
    }
    store.closeLocalSession(id)
    if (!store.hasSessions) {
      onNewSession()
    } else {
      syncUrl(store.activeSessionId)
    }
  }

  return {
    ensureSession,
    resetActiveSession,
    dismissError,
    handleResetFromError,
    onNewSession,
    setActiveSession,
    onCloseSession
  }
}
