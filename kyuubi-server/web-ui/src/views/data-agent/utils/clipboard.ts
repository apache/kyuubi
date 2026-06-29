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

import { ElMessage } from 'element-plus'
import { useI18n } from 'vue-i18n'
import { formatArgs } from './format'

function legacyCopy(text: string): boolean {
  const ta = document.createElement('textarea')
  ta.value = text
  ta.setAttribute('readonly', '')
  ta.style.position = 'fixed'
  ta.style.opacity = '0'
  document.body.appendChild(ta)
  ta.select()
  let ok = false
  try {
    ok = document.execCommand('copy')
  } catch {
    ok = false
  }
  document.body.removeChild(ta)
  return ok
}

export function useCopyText() {
  const { t } = useI18n()
  return async function copyText(text: unknown) {
    const payload =
      typeof text === 'string' ? text : text == null ? '' : formatArgs(text)
    let ok = false
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(payload)
        ok = true
      } catch {
        ok = false
      }
    }
    if (!ok) ok = legacyCopy(payload)
    if (ok) {
      ElMessage.success({ message: t('data_agent.copied'), duration: 1500 })
    } else {
      ElMessage.warning(t('data_agent.copy_failed'))
    }
  }
}
