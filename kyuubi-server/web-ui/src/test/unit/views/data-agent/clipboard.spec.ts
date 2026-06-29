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

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

const success = vi.fn()
const warning = vi.fn()
vi.mock('element-plus', () => ({
  ElMessage: {
    success: (...args: unknown[]) => success(...args),
    warning: (...args: unknown[]) => warning(...args)
  }
}))
vi.mock('vue-i18n', () => ({
  useI18n: () => ({ t: (k: string) => k })
}))

import { useCopyText } from '@/views/data-agent/utils/clipboard'

describe('useCopyText', () => {
  let execCommand: ReturnType<typeof vi.fn>
  let originalClipboard: PropertyDescriptor | undefined

  beforeEach(() => {
    success.mockClear()
    warning.mockClear()
    execCommand = vi.fn(() => true)
    document.execCommand = execCommand as unknown as typeof document.execCommand
    originalClipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard')
  })

  afterEach(() => {
    if (originalClipboard) {
      Object.defineProperty(navigator, 'clipboard', originalClipboard)
    } else {
      delete (navigator as { clipboard?: unknown }).clipboard
    }
  })

  function setClipboard(value: unknown) {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value
    })
  }

  it('uses navigator.clipboard when available', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    setClipboard({ writeText })
    await useCopyText()('hello')
    expect(writeText).toHaveBeenCalledWith('hello')
    expect(execCommand).not.toHaveBeenCalled()
    expect(success).toHaveBeenCalledOnce()
  })

  it('falls back to execCommand when clipboard API is unavailable', async () => {
    setClipboard(undefined)
    await useCopyText()('hello')
    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(success).toHaveBeenCalledOnce()
  })

  it('falls back to execCommand when clipboard API throws', async () => {
    setClipboard({ writeText: vi.fn().mockRejectedValue(new Error('denied')) })
    await useCopyText()('hello')
    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(success).toHaveBeenCalledOnce()
  })

  it('warns when both paths fail', async () => {
    setClipboard(undefined)
    execCommand.mockReturnValue(false)
    await useCopyText()('hello')
    expect(warning).toHaveBeenCalledOnce()
    expect(success).not.toHaveBeenCalled()
  })
})
