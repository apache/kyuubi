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

import { marked } from 'marked'
import DOMPurify from 'dompurify'

// Force external links to open in a new tab without window.opener leakage.
// Guarded so repeated module imports don't stack duplicate hooks.
const DP = DOMPurify as unknown as { __kyuubiLinkHook?: boolean }
if (!DP.__kyuubiLinkHook) {
  DOMPurify.addHook('afterSanitizeAttributes', (node) => {
    if ('tagName' in node && (node as Element).tagName === 'A') {
      const el = node as Element
      el.setAttribute('target', '_blank')
      el.setAttribute('rel', 'noopener noreferrer')
    }
  })
  DP.__kyuubiLinkHook = true
}

export function renderMarkdown(content: string): string {
  if (!content) return ''
  try {
    return DOMPurify.sanitize(marked.parse(content, { async: false }) as string)
  } catch {
    return DOMPurify.sanitize(content)
  }
}
