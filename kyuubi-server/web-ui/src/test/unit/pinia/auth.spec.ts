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

import { Buffer } from 'node:buffer'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import piniaPluginPersistedstate from 'pinia-plugin-persistedstate'
import { useAuthStore } from '@/pinia/auth/auth'
import request from '@/utils/request'

const fetchMock = vi.fn<typeof fetch>()

function activatePinia() {
  const pinia = createPinia()
  pinia.use(piniaPluginPersistedstate)
  createApp({}).use(pinia)
  setActivePinia(pinia)
}

describe('HTTP Basic credentials', () => {
  beforeEach(() => {
    localStorage.clear()
    activatePinia()
    fetchMock.mockReset().mockImplementation(async () => {
      return new Response(JSON.stringify('pong'), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })
    })
    vi.stubGlobal('fetch', fetchMock)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    localStorage.clear()
  })

  it.each([
    ['testaccount', 'password123!'],
    ['testaccount', '§'],
    ['testaccount', 'ß'],
    ['testaccount', 'ö'],
    ['testaccount', 'ä'],
    ['testaccount', 'ü'],
    ['testaccount', '€'],
    ['testaccount', '§ßöäü€'],
    ['testaccount', 'Case:§ßöäü€'],
    ['töstuser', '§ßöäü€'],
    ['用户', '密碼🔐']
  ])('preserves UTF-8 credentials for %s / %s', async (username, password) => {
    const store = useAuthStore()
    const expected = `Basic ${Buffer.from(
      `${username}:${password}`,
      'utf8'
    ).toString('base64')}`

    await store.setUser(username, password)

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      '/api/v1/ping',
      expect.objectContaining({ headers: { Authorization: expected } })
    )
    expect(store.user).toBe(username)
    expect(store.isAuthenticated).toBe(true)
    expect(store.authToken).toBe(expected)

    await request({ url: 'api/v1/sessions', method: 'get' })

    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/v1/sessions',
      expect.objectContaining({ headers: { Authorization: expected } })
    )
  })

  it('preserves UTF-8 credentials after restoring the persisted login', async () => {
    await useAuthStore().setUser('töstuser', '§ßöäü€')
    await nextTick()
    activatePinia()

    const restored = useAuthStore()
    expect(restored.user).toBe('töstuser')
    expect(restored.isAuthenticated).toBe(true)

    await request({ url: 'api/v1/sessions', method: 'get' })

    expect(fetchMock).toHaveBeenLastCalledWith(
      '/api/v1/sessions',
      expect.objectContaining({
        headers: {
          Authorization: `Basic ${Buffer.from(
            'töstuser:§ßöäü€',
            'utf8'
          ).toString('base64')}`
        }
      })
    )
  })

  it('does not save credentials when authentication fails', async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 403 }))
    const store = useAuthStore()

    await expect(store.setUser('testaccount', 'wrong')).rejects.toThrow()

    expect(store.user).toBeNull()
    expect(store.authToken).toBeNull()
    expect(store.isAuthenticated).toBe(false)
  })
})
