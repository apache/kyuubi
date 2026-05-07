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

import { describe, it, expect, beforeEach } from 'vitest'
import {
  sanitizeJdbcUrl,
  saveJdbcToHistory,
  loadJdbcHistory,
  removeJdbcFromHistory
} from '@/views/data-agent/utils/jdbc'

describe('sanitizeJdbcUrl', () => {
  describe('? & style (Trino / MySQL / Postgres)', () => {
    it('strips first sensitive param without breaking the ? separator', () => {
      expect(sanitizeJdbcUrl('jdbc:postgresql://h/db?password=p&user=u')).toBe(
        'jdbc:postgresql://h/db?user=u'
      )
    })

    it('strips middle sensitive param', () => {
      expect(
        sanitizeJdbcUrl('jdbc:mysql://h/db?user=u&password=p&useSSL=false')
      ).toBe('jdbc:mysql://h/db?user=u&useSSL=false')
    })

    it('strips trailing sensitive param', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://h/db?user=u&password=p')).toBe(
        'jdbc:mysql://h/db?user=u'
      )
    })

    it('strips multiple sensitive params at once', () => {
      expect(
        sanitizeJdbcUrl(
          'jdbc:trino://h:8080/c/s?password=p&user=u&token=t&pwd=q'
        )
      ).toBe('jdbc:trino://h:8080/c/s?user=u')
    })

    it('strips the only param without leaving a dangling ?', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://h/db?password=p')).toBe(
        'jdbc:mysql://h/db'
      )
    })
  })

  describe('; style (Hive2 / Kyuubi)', () => {
    it('strips trailing ;password without leaving dangling ;', () => {
      expect(sanitizeJdbcUrl('jdbc:hive2://h:10009/d;user=u;password=p')).toBe(
        'jdbc:hive2://h:10009/d;user=u'
      )
    })

    it('strips middle ;password and collapses to single ;', () => {
      expect(
        sanitizeJdbcUrl('jdbc:hive2://h:10009/d;user=u;password=p;ssl=true')
      ).toBe('jdbc:hive2://h:10009/d;user=u;ssl=true')
    })

    it('handles ;ssl=true;password=p (previously left dangling ;)', () => {
      expect(sanitizeJdbcUrl('jdbc:hive2://h/d;ssl=true;password=p')).toBe(
        'jdbc:hive2://h/d;ssl=true'
      )
    })
  })

  describe('alias keys', () => {
    it.each([
      'password',
      'pwd',
      'passwd',
      'token',
      'secret',
      'authToken',
      'accessToken',
      'oauth2Token'
    ])('strips %s (case-insensitive)', (key) => {
      expect(sanitizeJdbcUrl(`jdbc:x://h?${key}=v&user=u`)).toBe(
        'jdbc:x://h?user=u'
      )
      expect(sanitizeJdbcUrl(`jdbc:x://h?${key.toUpperCase()}=v&user=u`)).toBe(
        'jdbc:x://h?user=u'
      )
    })

    it('does not strip non-sensitive keys', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://h/db?user=u&useSSL=false')).toBe(
        'jdbc:mysql://h/db?user=u&useSSL=false'
      )
    })

    it('does not strip keys that merely contain the substring "password"', () => {
      expect(sanitizeJdbcUrl('jdbc:x://h?passwordPolicy=strict&user=u')).toBe(
        'jdbc:x://h?passwordPolicy=strict&user=u'
      )
    })
  })

  describe('userinfo form (user:pass@host)', () => {
    it('strips user:password before @host', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://alice:secret@h:3306/db')).toBe(
        'jdbc:mysql://h:3306/db'
      )
    })

    it('strips user:password and trailing sensitive query params', () => {
      expect(
        sanitizeJdbcUrl('jdbc:mysql://alice:secret@h/db?password=p&user=u')
      ).toBe('jdbc:mysql://h/db?user=u')
    })

    it('does not touch in-path @ characters', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://h/db?email=a@b.com')).toBe(
        'jdbc:mysql://h/db?email=a@b.com'
      )
    })
  })

  describe('edge cases', () => {
    it('returns empty string unchanged', () => {
      expect(sanitizeJdbcUrl('')).toBe('')
    })

    it('returns URL with no params unchanged', () => {
      expect(sanitizeJdbcUrl('jdbc:mysql://h/db')).toBe('jdbc:mysql://h/db')
    })

    it('handles empty value', () => {
      expect(sanitizeJdbcUrl('jdbc:x://h?password=&user=u')).toBe(
        'jdbc:x://h?user=u'
      )
    })

    it('is idempotent', () => {
      const once = sanitizeJdbcUrl('jdbc:mysql://h/db?password=p&user=u')
      expect(sanitizeJdbcUrl(once)).toBe(once)
    })
  })
})

describe('jdbc history (localStorage)', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('persists sanitized URL only', () => {
    saveJdbcToHistory('jdbc:mysql://h/db?user=u&password=p')
    expect(loadJdbcHistory()).toEqual(['jdbc:mysql://h/db?user=u'])
  })

  it('deduplicates and brings most-recent to front', () => {
    saveJdbcToHistory('jdbc:mysql://h/a?user=u')
    saveJdbcToHistory('jdbc:mysql://h/b?user=u')
    saveJdbcToHistory('jdbc:mysql://h/a?user=u')
    expect(loadJdbcHistory()).toEqual([
      'jdbc:mysql://h/a?user=u',
      'jdbc:mysql://h/b?user=u'
    ])
  })

  it('caps history at 10 entries', () => {
    for (let i = 0; i < 15; i++) {
      saveJdbcToHistory(`jdbc:mysql://h/db${i}?user=u`)
    }
    expect(loadJdbcHistory()).toHaveLength(10)
  })

  it('removes entries by exact match', () => {
    saveJdbcToHistory('jdbc:mysql://h/a?user=u')
    saveJdbcToHistory('jdbc:mysql://h/b?user=u')
    removeJdbcFromHistory('jdbc:mysql://h/a?user=u')
    expect(loadJdbcHistory()).toEqual(['jdbc:mysql://h/b?user=u'])
  })

  it('survives corrupt storage', () => {
    localStorage.setItem('data-agent-jdbc-history', 'not json')
    expect(loadJdbcHistory()).toEqual([])
  })
})
