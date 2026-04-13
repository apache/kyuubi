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

import { useAuthStore } from '@/pinia/auth/auth'

interface RequestConfig {
  url: string
  method: 'get' | 'post' | 'put' | 'delete'
  data?: unknown
  params?: Record<string, any>
  auth?: { username: string; password: string }
}

async function request(config: RequestConfig): Promise<unknown> {
  const { url, method, data, params, auth } = config
  const authStore = useAuthStore()

  const headers: Record<string, string> = {}

  if (auth) {
    headers['Authorization'] = `Basic ${btoa(
      auth.username + ':' + auth.password
    )}`
  } else if (authStore.isAuthenticated && authStore.authToken) {
    headers['Authorization'] = authStore.authToken
  }

  if (data !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  // Ensure absolute path to avoid resolving relative to the app's base path (/ui/)
  let fullUrl = url.startsWith('/') ? url : `/${url}`
  if (params) {
    const searchParams = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        searchParams.append(key, String(value))
      }
    }
    const queryString = searchParams.toString()
    if (queryString) {
      fullUrl = `${fullUrl}?${queryString}`
    }
  }

  const response = await fetch(fullUrl, {
    method: method.toUpperCase(),
    headers,
    body: data !== undefined ? JSON.stringify(data) : undefined
  })

  if (response.status === 401) {
    window.dispatchEvent(new CustomEvent('auth-required'))
    throw new Error('Unauthorized')
  }

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`)
  }

  const contentType = response.headers.get('content-type')
  if (!contentType || !contentType.includes('application/json')) {
    return undefined
  }

  const text = await response.text()
  return text ? JSON.parse(text) : undefined
}

export default request
