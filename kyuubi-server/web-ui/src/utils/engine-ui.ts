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

function normalizeEngineUIUrl(url: string): string {
  if (/^https?:\/\//i.test(url)) {
    return url
  }
  return `http://${url}`
}

type EngineUIResource = { kind: 'engines' | 'batches'; id: string }

function getProxyEngineUIUrl(resource: EngineUIResource): string {
  if (!resource.id || !/^[A-Za-z0-9_-]+$/.test(resource.id)) return ''
  return `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/${resource.kind}/${resource.id}/`
}

function getEngineUIUrl(
  url: string,
  proxyEnabled: boolean,
  resource?: EngineUIResource
): string {
  if (proxyEnabled) return resource ? getProxyEngineUIUrl(resource) : ''
  return url ? normalizeEngineUIUrl(url) : ''
}

function getEngineUIReturnTarget(value: string | null): string | undefined {
  if (
    !value ||
    !value.startsWith('/') ||
    value.startsWith('//') ||
    value.includes('\\')
  ) {
    return undefined
  }
  const target = new URL(value, window.location.origin)
  if (
    target.origin !== window.location.origin ||
    !/^\/engine-ui\/(engines|batches)\/[A-Za-z0-9_-]+(?:\/|$)/.test(
      target.pathname
    )
  ) {
    return undefined
  }
  return target.pathname + target.search + target.hash
}

export {
  getEngineUIUrl,
  getProxyEngineUIUrl,
  normalizeEngineUIUrl,
  getEngineUIReturnTarget
}
