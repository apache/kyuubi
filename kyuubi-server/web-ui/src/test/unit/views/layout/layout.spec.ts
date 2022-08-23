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

import { mount } from '@vue/test-utils'
import Layout from '@/views/layout/index.vue'
import { expect, test } from 'vitest'
import { createI18n, getStore } from '@/test/unit/utils'

test('mount component', () => {
  expect(Layout).toBeTruthy()

  const Aside = {
    template: '<div>aside</div>'
  }

  const wrapper = mount(Layout, {
    global: {
      plugins: [createI18n(), getStore()],
      stubs: {
        Aside
      }
    }
  })

  expect(wrapper.findAll('el-header')).toHaveLength(1)
  expect(wrapper.findAll('el-main')).toHaveLength(1)
})
