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

function getProxyEngineUIUrl(url: string): string {
  const engineURL = normalizeEngineUIUrl(url)
  const parsedURL = new URL(engineURL)
  const proxyPath = `${parsedURL.host}${parsedURL.pathname}${parsedURL.search}`
  return `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/${proxyPath}`
}

function getEngineUIUrl(url: string, proxyEnabled: boolean): string {
  if (proxyEnabled) {
    return getProxyEngineUIUrl(url)
  }
  return normalizeEngineUIUrl(url)
}

export { getEngineUIUrl, getProxyEngineUIUrl, normalizeEngineUIUrl }
