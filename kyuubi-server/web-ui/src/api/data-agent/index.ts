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

import request from '@/utils/request'
import { useAuthStore } from '@/pinia/auth/auth'
import { fetchEventSource } from '@microsoft/fetch-event-source'

export interface SessionOpenRequest {
  configs?: Record<string, string>
}

export interface SessionHandle {
  identifier: string
  secretId: string
}

export interface SseEvent {
  event: string
  data: string
}

export function openSession(data: SessionOpenRequest) {
  return request({
    url: 'api/v1/sessions',
    method: 'post',
    data
  })
}

export function closeSession(sessionHandle: string) {
  return request({
    url: `api/v1/sessions/${sessionHandle}`,
    method: 'delete'
  })
}

export function getSession(sessionHandle: string) {
  return request({
    url: `api/v1/sessions/${sessionHandle}`,
    method: 'get'
  })
}

export interface ApprovalResponse {
  status: 'ok' | 'not_found'
  action: 'approved' | 'denied'
  requestId: string
}

/**
 * Approve or deny a pending tool call.
 */
export function approveToolCall(
  sessionHandle: string,
  requestId: string,
  approved: boolean
): Promise<ApprovalResponse> {
  return request({
    url: `api/v1/data-agent/${sessionHandle}/approve`,
    method: 'post',
    data: { requestId, approved }
  })
}

/**
 * Send a chat message and receive SSE streaming response.
 * Uses @microsoft/fetch-event-source for robust SSE parsing and auth header support.
 */
export function chatStream(
  sessionHandle: string,
  text: string,
  onEvent: (event: SseEvent) => void,
  signal?: AbortSignal,
  approvalMode?: string
): Promise<void> {
  const body: Record<string, string> = { text }
  if (approvalMode) body.approvalMode = approvalMode

  const authStore = useAuthStore()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json'
  }
  if (authStore.isAuthenticated && authStore.authToken) {
    headers['Authorization'] = authStore.authToken
  }

  return fetchEventSource(`/api/v1/data-agent/${sessionHandle}/chat`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal,
    openWhenHidden: true,
    onmessage(ev) {
      if (ev.event) {
        onEvent({ event: ev.event, data: ev.data })
      }
    },
    onclose() {
      // Stream ended normally
    },
    onerror(err) {
      // Don't retry — propagate the error
      throw err
    }
  })
}
