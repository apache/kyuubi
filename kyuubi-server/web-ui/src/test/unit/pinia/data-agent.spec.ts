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

import { describe, it, expect, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import {
  useDataAgentStore,
  serializeSanitized,
  isValidApprovalMode
} from '@/pinia/data-agent'

describe('serializeSanitized (persist serializer)', () => {
  it('strips credentials from jdbcUrl on every session', () => {
    const out = JSON.parse(
      serializeSanitized({
        activeSessionId: 'a',
        sessionOrder: ['a', 'b'],
        sessions: {
          a: {
            id: 'a',
            jdbcUrl: 'jdbc:mysql://h/db?user=u&password=secret'
          },
          b: {
            id: 'b',
            jdbcUrl: 'jdbc:hive2://h/d;user=u;password=hunter2'
          }
        }
      })
    )
    expect(out.sessions.a.jdbcUrl).toBe('jdbc:mysql://h/db?user=u')
    expect(out.sessions.b.jdbcUrl).toBe('jdbc:hive2://h/d;user=u')
    const dump = JSON.stringify(out)
    expect(dump).not.toContain('secret')
    expect(dump).not.toContain('hunter2')
  })

  it('preserves non-credential session fields verbatim', () => {
    const out = JSON.parse(
      serializeSanitized({
        sessions: {
          a: {
            id: 'a',
            jdbcUrl: 'jdbc:mysql://h?password=p',
            model: 'qwen-plus',
            approvalMode: 'STRICT',
            title: 'my chat',
            messages: [{ id: 1, role: 'user', text: 'hi' }]
          }
        }
      })
    )
    expect(out.sessions.a).toMatchObject({
      model: 'qwen-plus',
      approvalMode: 'STRICT',
      title: 'my chat',
      messages: [{ id: 1, role: 'user', text: 'hi' }],
      jdbcUrl: 'jdbc:mysql://h'
    })
  })

  it('preserves activeSessionId and sessionOrder at top level', () => {
    const out = JSON.parse(
      serializeSanitized({
        activeSessionId: 'a',
        sessionOrder: ['a', 'b'],
        sessions: {}
      })
    )
    expect(out.activeSessionId).toBe('a')
    expect(out.sessionOrder).toEqual(['a', 'b'])
  })

  it('handles empty / missing jdbcUrl without crashing', () => {
    const out = JSON.parse(
      serializeSanitized({
        sessions: {
          a: { id: 'a', jdbcUrl: '' },
          b: { id: 'b' }
        }
      })
    )
    expect(out.sessions.a.jdbcUrl).toBe('')
    expect(out.sessions.b.jdbcUrl).toBe('')
  })

  it('handles empty sessions object', () => {
    const out = JSON.parse(
      serializeSanitized({
        activeSessionId: '',
        sessionOrder: [],
        sessions: {}
      })
    )
    expect(out.sessions).toEqual({})
  })

  it('handles missing sessions field gracefully', () => {
    const out = JSON.parse(serializeSanitized({ activeSessionId: '' }))
    expect(out.sessions).toEqual({})
  })

  it('output is idempotent under re-serialization', () => {
    const once = serializeSanitized({
      sessions: {
        a: { id: 'a', jdbcUrl: 'jdbc:mysql://h?user=u&password=p' }
      }
    })
    const twice = serializeSanitized(JSON.parse(once))
    expect(twice).toBe(once)
  })
})

describe('isValidApprovalMode', () => {
  it.each(['AUTO_APPROVE', 'NORMAL', 'STRICT'])('accepts %s', (m) => {
    expect(isValidApprovalMode(m)).toBe(true)
  })

  it.each([null, undefined, '', 'auto', 'NORMAL ', 'admin'])(
    'rejects %s',
    (m) => {
      expect(isValidApprovalMode(m as any)).toBe(false)
    }
  )
})

describe('data-agent store actions', () => {
  beforeEach(() => {
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  it('createSession appends to order and sets active', () => {
    const store = useDataAgentStore()
    const a = store.createSession()
    const b = store.createSession()
    expect(store.sessionOrder).toEqual([a, b])
    expect(store.activeSessionId).toBe(b)
  })

  it('orderedSessions returns newest-first', () => {
    const store = useDataAgentStore()
    const a = store.createSession()
    const b = store.createSession()
    const c = store.createSession()
    expect(store.orderedSessions.map((s) => s.id)).toEqual([c, b, a])
  })

  it('patchSession mutates in place and bumps updatedAt', async () => {
    const store = useDataAgentStore()
    const id = store.createSession()
    const before = store.sessions[id].updatedAt
    await new Promise((r) => setTimeout(r, 2))
    store.patchSession(id, { title: 'hi' })
    expect(store.sessions[id].title).toBe('hi')
    expect(store.sessions[id].updatedAt).toBeGreaterThan(before)
  })

  it('patchSession is a no-op for unknown id', () => {
    const store = useDataAgentStore()
    expect(() =>
      store.patchSession('nonexistent', { title: 'x' })
    ).not.toThrow()
  })

  it('hydrateTransientFlags clears streaming/initializing/approving', () => {
    const store = useDataAgentStore()
    const id = store.createSession()
    store.patchSession(id, {
      streaming: true,
      initializing: true,
      approvingRequestId: 'req-1'
    })
    store.hydrateTransientFlags()
    const s = store.sessions[id]
    expect(s.streaming).toBe(false)
    expect(s.initializing).toBe(false)
    expect(s.approvingRequestId).toBe('')
  })

  it('resetSession clears conversation but keeps slot', () => {
    const store = useDataAgentStore()
    const id = store.createSession()
    store.patchSession(id, { sessionHandle: 'h-1', title: 'something' })
    store.appendMessage(id, { id: 1, role: 'user', text: 'hi' })
    store.resetSession(id)
    const s = store.sessions[id]
    expect(s).toBeDefined()
    expect(s.sessionHandle).toBe('')
    expect(s.messages).toEqual([])
    expect(s.title).toBe('')
  })

  it('keeps raw jdbcUrl in memory (sanitization happens at persist boundary)', () => {
    const store = useDataAgentStore()
    const id = store.createSession()
    const raw = 'jdbc:mysql://h/db?user=u&password=secret'
    store.patchSession(id, { jdbcUrl: raw })
    // openSession reads this — must stay raw in memory
    expect(store.sessions[id].jdbcUrl).toBe(raw)
  })

  it('nextMsgId returns monotonic ids per session', () => {
    const store = useDataAgentStore()
    const id = store.createSession()
    expect(store.nextMsgId(id)).toBe(1)
    expect(store.nextMsgId(id)).toBe(2)
    expect(store.nextMsgId(id)).toBe(3)
  })
})
