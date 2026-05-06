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

export const JDBC_TEMPLATES = [
  {
    label: 'Spark / Hive (Thrift)',
    value:
      'jdbc:hive2://localhost:10009/default;user=username;password=password',
    isHistory: false
  },
  {
    label: 'Trino',
    value:
      'jdbc:trino://localhost:8080/catalog/schema?user=username&password=password',
    isHistory: false
  },
  {
    label: 'MySQL',
    value:
      'jdbc:mysql://localhost:3306/mydb?user=username&password=password&useSSL=false',
    isHistory: false
  }
]

const JDBC_HISTORY_KEY = 'data-agent-jdbc-history'
const SENSITIVE_PARAM = /^(password|pwd|passwd|token|secret|authtoken)$/i

// Strip credentials from JDBC URL. Handles both ?a=1&b=2 (Trino/MySQL)
// and ;a=1;b=2 (Hive2) parameter styles.
export function sanitizeJdbcUrl(url: string): string {
  return url
    .replace(/([?&;])([^=&;#]+)=([^&;#]*)/g, (match, sep, key) =>
      SENSITIVE_PARAM.test(key) ? sep : match
    )
    .replace(/([?&;])[?&;]+/g, '$1')
    .replace(/[?&;]$/, '')
}

export function loadJdbcHistory(): string[] {
  try {
    return JSON.parse(localStorage.getItem(JDBC_HISTORY_KEY) || '[]')
  } catch {
    return []
  }
}

export function saveJdbcToHistory(url: string) {
  const sanitized = sanitizeJdbcUrl(url.trim())
  if (!sanitized) return
  const history = loadJdbcHistory().filter((u) => u !== sanitized)
  history.unshift(sanitized)
  if (history.length > 10) history.length = 10
  try {
    localStorage.setItem(JDBC_HISTORY_KEY, JSON.stringify(history))
  } catch {
    /* quota exceeded or storage disabled — drop silently */
  }
}

export function removeJdbcFromHistory(url: string) {
  const history = loadJdbcHistory().filter((u) => u !== url)
  try {
    localStorage.setItem(JDBC_HISTORY_KEY, JSON.stringify(history))
  } catch {
    /* ignore */
  }
}

export interface JdbcSuggestion {
  label: string
  value: string
  isHistory: boolean
}

export function buildJdbcSuggestions(
  query: string,
  historyLabel: string
): JdbcSuggestion[] {
  const history = loadJdbcHistory()
  const templateValues = new Set(JDBC_TEMPLATES.map((tpl) => tpl.value))
  const historyItems: JdbcSuggestion[] = history
    .filter((u) => !templateValues.has(u))
    .map((u) => ({
      label: historyLabel,
      value: u,
      isHistory: true
    }))
  const all = [...historyItems, ...JDBC_TEMPLATES]
  if (!query) return all
  const q = query.toLowerCase()
  return all.filter(
    (item) =>
      item.value.toLowerCase().includes(q) ||
      item.label.toLowerCase().includes(q)
  )
}
