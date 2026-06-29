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

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { chatStream, SseEvent } from '@/api/data-agent'

// Mock the auth store
vi.mock('@/pinia/auth/auth', () => ({
  useAuthStore: () => ({ isAuthenticated: false, authToken: '' })
}))

// Capture the options passed to fetchEventSource so we can drive events manually
let capturedOptions: any = null
vi.mock('@microsoft/fetch-event-source', () => ({
  fetchEventSource: vi.fn((_url: string, opts: any) => {
    capturedOptions = opts
    return Promise.resolve()
  })
}))

describe('chatStream SSE via fetchEventSource', () => {
  beforeEach(() => {
    capturedOptions = null
  })

  it('forwards SSE events to the onEvent callback', async () => {
    const events: SseEvent[] = []
    const promise = chatStream('handle', 'hi', (e) => events.push(e))

    // Simulate the library calling onmessage
    capturedOptions.onmessage({
      event: 'content_delta',
      data: '{"text":"hello"}'
    })
    capturedOptions.onmessage({ event: 'done', data: '{}' })

    await promise

    expect(events).toEqual([
      { event: 'content_delta', data: '{"text":"hello"}' },
      { event: 'done', data: '{}' }
    ])
  })

  it('skips events without an event type', async () => {
    const events: SseEvent[] = []
    const promise = chatStream('handle', 'hi', (e) => events.push(e))

    capturedOptions.onmessage({ event: '', data: 'ignored' })
    capturedOptions.onmessage({ event: 'content_delta', data: '{"text":"ok"}' })

    await promise

    expect(events).toHaveLength(1)
    expect(events[0].event).toBe('content_delta')
  })

  it('passes approvalMode in the request body', async () => {
    const { fetchEventSource } = await import('@microsoft/fetch-event-source')
    await chatStream('handle', 'hi', () => {}, undefined, 'STRICT')

    expect(fetchEventSource).toHaveBeenCalledWith(
      '/api/v1/data-agent/handle/chat',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ text: 'hi', approvalMode: 'STRICT' })
      })
    )
  })

  it('passes abort signal through', async () => {
    const { fetchEventSource } = await import('@microsoft/fetch-event-source')
    const controller = new AbortController()
    await chatStream('handle', 'hi', () => {}, controller.signal)

    expect(fetchEventSource).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ signal: controller.signal })
    )
  })

  it('propagates errors via onerror by rethrowing', async () => {
    const promise = chatStream('handle', 'hi', () => {})
    await promise // fetchEventSource is mocked to resolve immediately

    // Verify onerror rethrows
    const err = new Error('connection lost')
    expect(() => capturedOptions.onerror(err)).toThrow('connection lost')
  })

  it('handles mixed event types', async () => {
    const events: SseEvent[] = []
    const promise = chatStream('handle', 'hi', (e) => events.push(e))

    capturedOptions.onmessage({
      event: 'content_delta',
      data: '{"text":"Let me check"}'
    })
    capturedOptions.onmessage({
      event: 'tool_call',
      data: '{"name":"sql","args":"SELECT 1"}'
    })
    capturedOptions.onmessage({
      event: 'tool_result',
      data: '{"name":"sql","output":"1"}'
    })
    capturedOptions.onmessage({ event: 'done', data: '{}' })

    await promise

    expect(events.map((e) => e.event)).toEqual([
      'content_delta',
      'tool_call',
      'tool_result',
      'done'
    ])
  })
})
