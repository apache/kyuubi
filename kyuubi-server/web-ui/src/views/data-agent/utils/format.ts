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

export function formatTokens(n: number): string {
  if (n >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + 'k'
  return String(n)
}

export function formatArgs(args: unknown): string {
  if (args == null) return ''
  if (typeof args === 'string') {
    try {
      return JSON.stringify(JSON.parse(args), null, 2)
    } catch {
      return args
    }
  }
  try {
    return JSON.stringify(args, null, 2)
  } catch {
    return String(args)
  }
}

export function formatArgsForDisplay(args: unknown): string {
  if (args == null) return ''
  let value: unknown = args
  if (typeof args === 'string') {
    try {
      value = JSON.parse(args)
    } catch {
      return args
    }
  }
  return prettyPrintLoose(value, 0)
}

// Display-only formatter; formatArgs remains the copy-safe JSON path.
function prettyPrintLoose(value: unknown, indent: number): string {
  const pad = '  '.repeat(indent)
  const padInner = '  '.repeat(indent + 1)
  if (value === null) return 'null'
  if (typeof value === 'string') {
    if (value.includes('\n')) {
      const lines = value.split('\n').map((l) => padInner + l)
      return '"\n' + lines.join('\n') + '\n' + pad + '"'
    }
    return JSON.stringify(value)
  }
  if (typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]'
    const items = value.map((v) => padInner + prettyPrintLoose(v, indent + 1))
    return '[\n' + items.join(',\n') + '\n' + pad + ']'
  }
  const entries = Object.entries(value as Record<string, unknown>)
  if (entries.length === 0) return '{}'
  const items = entries.map(
    ([k, v]) =>
      padInner + JSON.stringify(k) + ': ' + prettyPrintLoose(v, indent + 1)
  )
  return '{\n' + items.join(',\n') + '\n' + pad + '}'
}
