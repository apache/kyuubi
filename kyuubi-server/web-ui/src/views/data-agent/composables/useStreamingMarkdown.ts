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

import { ref, watch, onBeforeUnmount, type Ref } from 'vue'
import { renderMarkdown } from '../utils/markdown'

// Throttled markdown render for a streaming text source.
// Re-parsing marked+DOMPurify on every SSE delta is O(n²) and janks the UI;
// cap to ~12fps while streaming, then flush the full render on completion.
export function useStreamingMarkdown(
  text: () => string,
  streaming: () => boolean
): Ref<string> {
  const STREAM_RENDER_INTERVAL = 80
  const html = ref('')
  let throttleTimer: ReturnType<typeof setTimeout> | null = null
  let lastRenderAt = 0

  function flush() {
    html.value = renderMarkdown(text())
    lastRenderAt = Date.now()
    if (throttleTimer) {
      clearTimeout(throttleTimer)
      throttleTimer = null
    }
  }

  // Render initial content immediately so a block that mounts mid-stream
  // doesn't show blank until the next delta arrives.
  flush()

  watch(text, () => {
    if (!streaming()) {
      flush()
      return
    }
    const elapsed = Date.now() - lastRenderAt
    if (elapsed >= STREAM_RENDER_INTERVAL) {
      flush()
    } else if (!throttleTimer) {
      throttleTimer = setTimeout(flush, STREAM_RENDER_INTERVAL - elapsed)
    }
  })

  // When streaming flips false, flush immediately so the final text is rendered.
  watch(streaming, (s) => {
    if (!s) flush()
  })

  onBeforeUnmount(() => {
    if (throttleTimer) clearTimeout(throttleTimer)
  })

  return html
}
