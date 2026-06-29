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

import { ref, onBeforeUnmount } from 'vue'

export function useChatScroll() {
  const messagesContainer = ref<HTMLDivElement>()
  let scrollRafId: number | null = null
  let stickToBottom = true
  let programmaticScroll = false

  function onMessagesScroll() {
    if (programmaticScroll) return
    const el = messagesContainer.value
    if (!el) return
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight
    stickToBottom = distanceFromBottom < 40
  }

  function scrollToBottom(force = false) {
    if (!force && !stickToBottom) return
    if (scrollRafId) {
      if (!force) return
      cancelAnimationFrame(scrollRafId)
    }
    scrollRafId = requestAnimationFrame(() => {
      scrollRafId = null
      const el = messagesContainer.value
      if (!el) return
      programmaticScroll = true
      el.scrollTop = el.scrollHeight
      stickToBottom = true
      requestAnimationFrame(() => {
        programmaticScroll = false
      })
    })
  }

  function anchorToBottom() {
    stickToBottom = true
    scrollToBottom(true)
  }

  onBeforeUnmount(() => {
    if (scrollRafId) cancelAnimationFrame(scrollRafId)
  })

  return { messagesContainer, scrollToBottom, onMessagesScroll, anchorToBottom }
}
