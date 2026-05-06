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

import { defineStore } from 'pinia'
import type { ChatBlock } from '@/views/data-agent/types'
import { sanitizeJdbcUrl } from '@/views/data-agent/utils/jdbc'

export interface DataAgentTokenUsage {
  accumulatedPrompt: number
  accumulatedCompletion: number
  lastPrompt: number
  lastCompletion: number
  steps?: number
}

export interface DataAgentMessage {
  id: number
  role: 'user' | 'assistant'
  text?: string
  blocks?: ChatBlock[]
  usage?: DataAgentTokenUsage
}

export interface DataAgentSession {
  id: string
  title: string
  sessionHandle: string
  messages: DataAgentMessage[]
  msgIdCounter: number
  jdbcUrl: string
  approvalMode: string
  model: string
  streaming: boolean
  initializing: boolean
  errorMessage: string
  errorCanReset: boolean
  approvingRequestId: string
  createdAt: number
  updatedAt: number
}

interface DataAgentState {
  activeSessionId: string
  sessions: Record<string, DataAgentSession>
  sessionOrder: string[]
}

export const APPROVAL_MODES = ['AUTO_APPROVE', 'NORMAL', 'STRICT'] as const

export function isValidApprovalMode(v: string | null | undefined): boolean {
  return !!v && (APPROVAL_MODES as readonly string[]).includes(v)
}

function genId(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 8)
}

function buildSession(initial?: Partial<DataAgentSession>): DataAgentSession {
  const now = Date.now()
  return {
    id: initial?.id ?? genId(),
    title: '',
    sessionHandle: '',
    messages: [],
    msgIdCounter: 0,
    jdbcUrl: '',
    approvalMode: 'NORMAL',
    model: '',
    streaming: false,
    initializing: false,
    errorMessage: '',
    errorCanReset: false,
    approvingRequestId: '',
    createdAt: now,
    updatedAt: now,
    ...initial
  }
}

// Cap a single block string field. Tool results from SELECTs can be megabytes; persisting
// them verbatim quickly fills the 5MB sessionStorage quota and silently drops further
// writes. Truncated payload is enough to recognize the block; full content lives in memory
// during the active session.
const MAX_PERSIST_FIELD_CHARS = 2000
// Keep the most recent N messages with full block payloads. Older messages keep their
// metadata (role, text preview) so the UI can still render the conversation outline after
// a refresh, but their blocks are dropped.
const FULL_PERSIST_MESSAGE_COUNT = 20
const TRUNCATE_MARKER = '… [truncated]'

function truncate(s: string | undefined): string | undefined {
  if (s == null) return s
  if (s.length <= MAX_PERSIST_FIELD_CHARS) return s
  return s.slice(0, MAX_PERSIST_FIELD_CHARS) + TRUNCATE_MARKER
}

function slimBlock(b: ChatBlock): ChatBlock {
  const out: ChatBlock = { ...b }
  if (typeof out.text === 'string') out.text = truncate(out.text)
  if (typeof out.result === 'string') out.result = truncate(out.result)
  return out
}

export function slimMessagesForPersist(
  messages: DataAgentMessage[]
): DataAgentMessage[] {
  if (!messages || messages.length === 0) return messages
  const total = messages.length
  return messages.map((m, i) => {
    const isRecent = i >= total - FULL_PERSIST_MESSAGE_COUNT
    if (isRecent) {
      // Recent: keep blocks but cap individual long fields.
      return {
        ...m,
        text: typeof m.text === 'string' ? truncate(m.text) : m.text,
        blocks: m.blocks?.map(slimBlock)
      }
    }
    // Older: drop blocks entirely, keep a short text preview so the outline renders.
    return {
      id: m.id,
      role: m.role,
      text: truncate(m.text),
      usage: m.usage
    }
  })
}

// Strip credentials from every session's jdbcUrl before writing to sessionStorage. Also
// trim message blocks: long tool outputs (e.g. SELECT results) can blow past the 5MB
// sessionStorage quota and silently drop further writes. Raw URL and full message content
// stay in memory for the active session; the persisted copy is sanitized and slimmed.
export function serializeSanitized(state: any): string {
  const sessions = (state.sessions || {}) as Record<string, DataAgentSession>
  const sanitized: Record<string, DataAgentSession> = {}
  for (const [k, v] of Object.entries(sessions)) {
    sanitized[k] = {
      ...v,
      jdbcUrl: sanitizeJdbcUrl(v.jdbcUrl || ''),
      messages: slimMessagesForPersist(v.messages || [])
    }
  }
  return JSON.stringify({ ...state, sessions: sanitized })
}

export const useDataAgentStore = defineStore('data-agent', {
  state: (): DataAgentState => ({
    activeSessionId: '',
    sessions: {},
    sessionOrder: []
  }),
  getters: {
    activeSession(state): DataAgentSession | undefined {
      return state.sessions[state.activeSessionId]
    },
    // Newest sessions on top. We keep `sessionOrder` in creation order internally and
    // only flip the order at the read boundary so positions in the UI stay stable
    // (existing items don't shuffle when a new conversation is created).
    orderedSessions(state): DataAgentSession[] {
      const out: DataAgentSession[] = []
      for (let i = state.sessionOrder.length - 1; i >= 0; i--) {
        const s = state.sessions[state.sessionOrder[i]]
        if (s) out.push(s)
      }
      return out
    },
    hasSessions(state): boolean {
      return state.sessionOrder.length > 0
    }
  },
  actions: {
    // Drop transient flags that may have been persisted while a stream was active
    // before the page unloaded. Any in-flight HTTP/SSE was killed by the unload, so
    // restoring `streaming: true` would leave the UI stuck.
    hydrateTransientFlags() {
      for (const id of this.sessionOrder) {
        const s = this.sessions[id]
        if (!s) continue
        s.streaming = false
        s.initializing = false
        s.approvingRequestId = ''
      }
    },
    createSession(initial?: Partial<DataAgentSession>): string {
      const session = buildSession(initial)
      this.sessions[session.id] = session
      if (!this.sessionOrder.includes(session.id)) {
        this.sessionOrder.push(session.id)
      }
      this.activeSessionId = session.id
      return session.id
    },
    setActive(id: string) {
      if (this.sessions[id]) this.activeSessionId = id
    },
    patchSession(id: string, patch: Partial<DataAgentSession>) {
      const s = this.sessions[id]
      if (!s) return
      Object.assign(s, patch)
      s.updatedAt = Date.now()
    },
    appendMessage(id: string, message: DataAgentMessage) {
      const s = this.sessions[id]
      if (!s) return
      s.messages.push(message)
      s.updatedAt = Date.now()
    },
    nextMsgId(id: string): number {
      const s = this.sessions[id]
      if (!s) return 0
      s.msgIdCounter += 1
      return s.msgIdCounter
    },
    bindBackendHandle(id: string, handle: string) {
      const s = this.sessions[id]
      if (s) s.sessionHandle = handle
    },
    // Reset conversation state on a session while keeping its slot in the list.
    resetSession(id: string) {
      const s = this.sessions[id]
      if (!s) return
      s.sessionHandle = ''
      s.messages = []
      s.msgIdCounter = 0
      s.title = ''
      s.errorMessage = ''
      s.errorCanReset = false
      s.streaming = false
      s.initializing = false
      s.approvingRequestId = ''
      s.updatedAt = Date.now()
    },
    closeLocalSession(id: string) {
      delete this.sessions[id]
      this.sessionOrder = this.sessionOrder.filter((sid) => sid !== id)
      if (this.activeSessionId === id) {
        this.activeSessionId = this.sessionOrder[0] ?? ''
      }
    },
    setTitleIfEmpty(id: string, text: string) {
      const s = this.sessions[id]
      if (!s || s.title) return
      s.title = text.trim().slice(0, 32)
      s.updatedAt = Date.now()
    },
    renameSession(id: string, title: string) {
      const s = this.sessions[id]
      if (!s) return
      s.title = title.trim().slice(0, 64)
      s.updatedAt = Date.now()
    }
  },
  persist: {
    key: 'data-agent-sessions',
    storage: sessionStorage,
    paths: ['activeSessionId', 'sessions', 'sessionOrder'],
    serializer: {
      serialize: serializeSanitized,
      deserialize: JSON.parse
    }
  }
})
