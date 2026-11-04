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

import { afterEach, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useAuthStore } from '@/pinia/auth/auth'
import request from '@/utils/request'

afterEach(() => vi.unstubAllGlobals())

it.each(['success', 'failure'])(
  'clears local credentials before logout %s',
  async (outcome) => {
    setActivePinia(createPinia())
    const store = useAuthStore()
    store.$patch({
      user: 'alice',
      authToken: 'Basic test',
      isAuthenticated: true
    })
    let finish!: (response: Response) => void
    let fail!: (error: Error) => void
    const pending = new Promise<Response>((resolve, reject) => {
      finish = resolve
      fail = reject
    })
    const fetch = vi
      .fn()
      .mockReturnValueOnce(pending)
      .mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetch)

    const logout = store.clearUser()
    expect(fetch.mock.calls[0][1].headers.Authorization).toBe('Basic test')
    expect(store.$state).toEqual({
      user: null,
      authToken: null,
      isAuthenticated: false
    })
    await request({ url: 'api/v1/ping', method: 'get' })
    expect(fetch.mock.calls[1][1].headers.Authorization).toBeUndefined()

    if (outcome === 'success') finish(new Response(null, { status: 204 }))
    else fail(new Error('Connection lost'))
    await logout
    expect(store.isAuthenticated).toBe(false)
  }
)
