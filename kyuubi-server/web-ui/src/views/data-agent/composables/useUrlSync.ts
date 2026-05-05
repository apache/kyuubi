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

import { watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import type { useDataAgentStore } from '@/pinia/data-agent'

const URL_QUERY_KEY = 'conversation'

export function useUrlSync(store: ReturnType<typeof useDataAgentStore>) {
  const route = useRoute()
  const router = useRouter()

  function syncUrl(id: string | null) {
    const target = id || undefined
    const current =
      (route.query[URL_QUERY_KEY] as string | undefined) ?? undefined
    if (current === target) return
    router.replace({
      query: { ...route.query, [URL_QUERY_KEY]: target }
    })
  }

  function readUrlSessionId(): string {
    return typeof route.query[URL_QUERY_KEY] === 'string'
      ? (route.query[URL_QUERY_KEY] as string)
      : ''
  }

  watch(
    () => route.query[URL_QUERY_KEY],
    (next) => {
      const id = typeof next === 'string' ? next : ''
      if (id && store.sessions[id] && id !== store.activeSessionId) {
        store.setActive(id)
      }
    }
  )

  return { syncUrl, readUrlSessionId }
}
