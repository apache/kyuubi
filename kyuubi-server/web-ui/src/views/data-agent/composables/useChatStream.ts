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

import { onBeforeUnmount } from 'vue'
import { useI18n } from 'vue-i18n'
import { chatStream, approveToolCall } from '@/api/data-agent'
import { type DataAgentMessage, useDataAgentStore } from '@/pinia/data-agent'

// AbortController and watchdog timer can't be serialised, and they belong to in-flight
// network resources rather than persisted UI state. Keep them in a registry keyed by
// session id so a stream survives session switching.
interface StreamCtx {
  abortController: AbortController
  watchdogTimer: ReturnType<typeof setTimeout> | null
}

const STREAM_SILENCE_TIMEOUT_MS = 30000

export function useStreamRegistry() {
  const streamContexts = new Map<string, StreamCtx>()

  function cancelStream(id: string) {
    const ctx = streamContexts.get(id)
    if (!ctx) return
    if (ctx.watchdogTimer) clearTimeout(ctx.watchdogTimer)
    ctx.abortController.abort()
    streamContexts.delete(id)
  }

  return { streamContexts, cancelStream }
}

export type StreamRegistry = ReturnType<typeof useStreamRegistry>

export function useChatStream(opts: {
  store: ReturnType<typeof useDataAgentStore>
  registry: StreamRegistry
  ensureSession: (id: string) => Promise<boolean>
  scrollToBottom: () => void
  anchorToBottom: () => void
}) {
  const { store, registry, ensureSession, scrollToBottom, anchorToBottom } =
    opts
  const { t } = useI18n()
  let isUnmounted = false

  async function sendMessage(text: string) {
    const id = store.activeSessionId
    if (!id) return
    const s = store.sessions[id]
    if (!s || s.streaming || s.initializing) return
    if (!(await ensureSession(id))) return
    // After ensureSession, store may have updated handle; re-read.
    const sNow = store.sessions[id]
    if (!sNow) return

    store.appendMessage(id, {
      id: store.nextMsgId(id),
      role: 'user',
      text
    })
    store.appendMessage(id, {
      id: store.nextMsgId(id),
      role: 'assistant',
      blocks: []
    })
    store.setTitleIfEmpty(id, text)
    const assistantMsg = sNow.messages[store.sessions[id]!.messages.length - 1]

    if (id === store.activeSessionId) {
      anchorToBottom()
    }
    await startStreaming(id, assistantMsg, text)
  }

  async function startStreaming(
    sessionId: string,
    assistantMsg: DataAgentMessage,
    text: string
  ) {
    const s = store.sessions[sessionId]
    if (!s) return
    store.patchSession(sessionId, {
      streaming: true,
      errorMessage: '',
      errorCanReset: false
    })
    const ctx: StreamCtx = {
      abortController: new AbortController(),
      watchdogTimer: null
    }
    registry.streamContexts.set(sessionId, ctx)
    let gotDone = false
    let watchdogFired = false

    const armWatchdog = () => {
      if (ctx.watchdogTimer) clearTimeout(ctx.watchdogTimer)
      ctx.watchdogTimer = setTimeout(() => {
        watchdogFired = true
        store.patchSession(sessionId, {
          errorMessage: t('data_agent.engine_unresponsive'),
          errorCanReset: true
        })
        ctx.abortController.abort()
      }, STREAM_SILENCE_TIMEOUT_MS)
    }

    armWatchdog()

    try {
      await chatStream(
        s.sessionHandle,
        text,
        (event) => {
          armWatchdog()
          if (event.event === 'done') gotDone = true
          handleSseEvent(sessionId, assistantMsg, event)
        },
        ctx.abortController.signal,
        s.approvalMode,
        s.model.trim() || undefined
      )
      if (
        !gotDone &&
        !watchdogFired &&
        !ctx.abortController.signal.aborted &&
        !store.sessions[sessionId]?.errorMessage
      ) {
        // eslint-disable-next-line no-console
        console.warn('SSE closed without `done` event')
        store.patchSession(sessionId, {
          errorMessage: t('data_agent.stream_incomplete'),
          errorCanReset: true
        })
      }
    } catch (e: any) {
      if (watchdogFired) {
        // already surfaced
      } else if (e.name !== 'AbortError') {
        store.patchSession(sessionId, {
          errorMessage: e.message || t('data_agent.stream_error')
        })
      }
    } finally {
      if (ctx.watchdogTimer) clearTimeout(ctx.watchdogTimer)
      registry.streamContexts.delete(sessionId)
      if (!isUnmounted) {
        store.patchSession(sessionId, { streaming: false })
      }
    }
  }

  function handleSseEvent(
    sessionId: string,
    msg: DataAgentMessage,
    event: { event: string; data: string }
  ) {
    if (isUnmounted) return
    if (event.event === 'ping') return
    const blocks = msg.blocks!
    let parsed: any
    try {
      parsed = JSON.parse(event.data)
    } catch {
      // eslint-disable-next-line no-console
      console.warn('Invalid SSE event data:', event.data)
      store.patchSession(sessionId, {
        errorMessage: t('data_agent.malformed_response')
      })
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
      case 'reasoning_delta': {
        const text = parsed.text || ''
        if (!text) break
        const last = blocks[blocks.length - 1]
        if (last && last.type === 'reasoning') {
          last.text = (last.text || '') + text
        } else {
          blocks.push({ type: 'reasoning', text, expanded: true })
        }
        break
      }
      case 'tool_call': {
        const toolCallId = parsed.id
        if (!toolCallId || !parsed.name) break
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
      case 'agent_finish':
        msg.usage = {
          accumulatedPrompt: Number(parsed.accumulatedPromptTokens) || 0,
          accumulatedCompletion:
            Number(parsed.accumulatedCompletionTokens) || 0,
          lastPrompt: Number(parsed.lastPromptTokens) || 0,
          lastCompletion: Number(parsed.lastCompletionTokens) || 0,
          steps: Number(parsed.steps) || undefined
        }
        // Auto-collapse reasoning blocks once the answer is in — they're large and
        // distract from the result. User can re-expand to inspect.
        for (const b of blocks) {
          if (b.type === 'reasoning') b.expanded = false
        }
        break
      case 'error':
        store.patchSession(sessionId, {
          errorMessage: parsed.message || t('data_agent.unknown_error')
        })
        break
      case 'done':
        break
    }
    if (sessionId === store.activeSessionId) scrollToBottom()
  }

  async function handleApproval(requestId: string, approved: boolean) {
    const id = store.activeSessionId
    if (!id) return
    const s = store.sessions[id]
    if (!s || !s.sessionHandle || s.approvingRequestId) return
    store.patchSession(id, { approvingRequestId: requestId })

    const setApprovalStatus = (
      status: 'pending' | 'approved' | 'denied',
      onlyIfPending = false
    ): boolean => {
      let changed = false
      for (const m of s.messages) {
        if (!m.blocks) continue
        for (const block of m.blocks) {
          if (
            block.type === 'approval_request' &&
            block.requestId === requestId
          ) {
            if (onlyIfPending && block.approvalStatus !== 'pending') {
              return false
            }
            block.approvalStatus = status
            changed = true
          }
        }
      }
      return changed
    }

    if (!setApprovalStatus(approved ? 'approved' : 'denied', true)) {
      store.patchSession(id, { approvingRequestId: '' })
      return
    }

    try {
      const res = await approveToolCall(s.sessionHandle, requestId, approved)
      if (res && res.status !== 'ok') {
        store.patchSession(id, {
          errorMessage: t('data_agent.approval_not_found')
        })
        setApprovalStatus('pending')
      }
    } catch (e: any) {
      store.patchSession(id, {
        errorMessage: t('data_agent.approval_failed', { message: e.message })
      })
      setApprovalStatus('pending')
    } finally {
      store.patchSession(id, { approvingRequestId: '' })
    }
  }

  function cancelActiveStream() {
    if (store.activeSessionId) registry.cancelStream(store.activeSessionId)
  }

  onBeforeUnmount(() => {
    isUnmounted = true
    for (const id of Array.from(registry.streamContexts.keys())) {
      registry.cancelStream(id)
    }
  })

  return { sendMessage, handleApproval, cancelActiveStream }
}
