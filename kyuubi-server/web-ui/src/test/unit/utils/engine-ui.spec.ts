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

import {
  getEngineUIUrl,
  getProxyEngineUIUrl,
  normalizeEngineUIUrl
} from '@/utils/engine-ui'
import { expect, test } from 'vitest'

test('engine ui url direct mode', () => {
  expect(normalizeEngineUIUrl('host:4040')).toEqual('http://host:4040')
  expect(getEngineUIUrl('https://host:4040/sql', false)).toEqual(
    'https://host:4040/sql'
  )
})

test('engine ui url proxy mode', () => {
  expect(getProxyEngineUIUrl('spark.example.com:4040')).toEqual(
    `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/spark.example.com:4040/`
  )
  expect(getEngineUIUrl('spark.example.com:4040/sql/?id=1', true)).toEqual(
    `${
      import.meta.env.VITE_APP_DEV_WEB_URL
    }engine-ui/spark.example.com:4040/sql/?id=1`
  )
})
