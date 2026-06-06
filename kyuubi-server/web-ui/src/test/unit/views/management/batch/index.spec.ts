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
import Batch from '@/views/management/batch/index.vue'
import { flushPromises, shallowMount } from '@vue/test-utils'
import { createRouter, createWebHistory } from 'vue-router'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import { createI18n, getStore } from '@/test/unit/utils'
import * as batchApi from '@/api/batch'
import * as serverApi from '@/api/server'
import { IBatch } from '@/api/batch/types'

vi.mock('@/api/batch')
vi.mock('@/api/server')

function makeBatch(overrides: Partial<IBatch>): IBatch {
  return {
    id: 'b1',
    user: 'anonymous',
    batchType: 'SPARK',
    name: 'b1',
    appStartTime: 0,
    appId: null,
    appUrl: null,
    appState: null,
    appDiagnostic: null,
    kyuubiInstance: '',
    state: 'PENDING',
    createTime: 1,
    endTime: 0,
    ...overrides
  }
}

function mountBatch() {
  const mockRouter = createRouter({ history: createWebHistory(), routes: [] })
  return shallowMount(Batch, {
    global: {
      plugins: [createI18n(), mockRouter, getStore(), ElementPlus]
    }
  })
}

beforeEach(() => {
  // init() runs on setup; give every API call a resolved default.
  vi.mocked(serverApi.getWebUIConfig).mockResolvedValue({
    engineUIProxyEnabled: true
  })
  vi.mocked(batchApi.getAllBatches).mockResolvedValue({
    from: 0,
    total: 0,
    batches: []
  })
  vi.mocked(batchApi.getBatch).mockResolvedValue(makeBatch({}))
  vi.mocked(batchApi.getBatchLocalLog).mockResolvedValue({
    logRowSet: [],
    rowCount: 0
  })
  vi.mocked(batchApi.deleteBatch).mockResolvedValue({
    success: true,
    msg: null
  })
})

afterEach(() => {
  vi.clearAllMocks()
  vi.useRealTimers()
})

test('getAppUI proxies the engine url only when proxy is enabled', () => {
  const wrapper = mountBatch()

  wrapper.vm.engineUIProxyConfig.engineUIProxyEnabled = false
  expect(wrapper.vm.getAppUI('host:4040')).toBe('http://host:4040')

  wrapper.vm.engineUIProxyConfig.engineUIProxyEnabled = true
  expect(wrapper.vm.getAppUI('host:4040')).toBe(
    `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/host:4040/`
  )
})

test('isTerminalState classifies batch states', () => {
  const wrapper = mountBatch()
  for (const state of ['FINISHED', 'ERROR', 'CANCELED']) {
    expect(wrapper.vm.isTerminalState(state)).toBe(true)
  }
  for (const state of ['PENDING', 'RUNNING']) {
    expect(wrapper.vm.isTerminalState(state)).toBe(false)
  }
})

test('driver UI link is shown only for a live batch that has an appUrl', () => {
  const wrapper = mountBatch()

  // live + appUrl => shown
  expect(
    wrapper.vm.canShowAppUI(makeBatch({ state: 'RUNNING', appUrl: 'h:4040' }))
  ).toBe(true)

  // terminal => hidden even when an appUrl is still persisted
  for (const state of ['FINISHED', 'ERROR', 'CANCELED']) {
    expect(
      wrapper.vm.canShowAppUI(makeBatch({ state, appUrl: 'h:4040' }))
    ).toBe(false)
  }

  // no appUrl => hidden
  expect(
    wrapper.vm.canShowAppUI(makeBatch({ state: 'RUNNING', appUrl: null }))
  ).toBe(false)
})

test('opening logs of a live batch tails incrementally and stops at terminal', async () => {
  vi.useFakeTimers()
  const wrapper = mountBatch()
  await flushPromises()

  vi.mocked(batchApi.getBatchLocalLog)
    .mockResolvedValueOnce({ logRowSet: ['l0', 'l1'], rowCount: 2 }) // open: initial drain
    .mockResolvedValueOnce({ logRowSet: ['l2'], rowCount: 1 }) // tail tick drain
    .mockResolvedValue({ logRowSet: [], rowCount: 0 }) // caught up
  vi.mocked(batchApi.getBatch).mockResolvedValue(
    makeBatch({ id: 'bx', state: 'FINISHED' })
  )

  wrapper.vm.openLog(
    makeBatch({ id: 'bx', state: 'RUNNING', appUrl: 'h:4040' })
  )
  await flushPromises()

  expect(wrapper.vm.logFollowing).toBe(true)
  expect(wrapper.vm.batchLogs).toEqual(['l0', 'l1'])
  // first fetch uses an absolute offset starting at 0
  expect(vi.mocked(batchApi.getBatchLocalLog).mock.calls[0]).toEqual([
    'bx',
    0,
    1000
  ])

  // one poll interval later: the cursor advanced to 2, then the batch is seen
  // terminal so following stops.
  await vi.advanceTimersByTimeAsync(2000)
  await flushPromises()

  const offsets = vi
    .mocked(batchApi.getBatchLocalLog)
    .mock.calls.map((c) => c[1])
  expect(offsets).toContain(2)
  expect(wrapper.vm.batchLogs).toContain('l2')
  expect(wrapper.vm.logFollowing).toBe(false)
})

test('log buffer is capped and flagged as truncated', async () => {
  const wrapper = mountBatch()
  await flushPromises()

  const fullChunk = Array.from({ length: 1000 }, (_, i) => `row-${i}`)
  vi.mocked(batchApi.getBatchLocalLog)
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 })
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 })
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 })
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 })
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 })
    .mockResolvedValueOnce({ logRowSet: fullChunk, rowCount: 1000 }) // 6000 fetched
    .mockResolvedValue({ logRowSet: [], rowCount: 0 })

  // terminal batch => openLog drains the whole backlog without starting a poll
  wrapper.vm.openLog(
    makeBatch({ id: 'bcap', state: 'FINISHED', appUrl: 'h:4040' })
  )
  await flushPromises()

  expect(wrapper.vm.batchLogs.length).toBe(5000)
  expect(wrapper.vm.logTruncated).toBe(true)
  expect(wrapper.vm.logFollowing).toBe(false)
})
