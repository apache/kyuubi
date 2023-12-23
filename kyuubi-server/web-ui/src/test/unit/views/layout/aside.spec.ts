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
import Aside from '@/layout/components/aside/index.vue'
import { expect, test } from 'vitest'
import { getStore } from '@/test/unit/utils'
import { createRouter, createWebHistory } from 'vue-router'

test('mount component', () => {
  expect(Aside).toBeTruthy()

  const mockRouter = createRouter({ history: createWebHistory(), routes: [] })
  mockRouter.currentRoute.value.params = {
    path: '/overview'
  }

  const wrapper = mount(Aside, {
    global: {
      plugins: [mockRouter, getStore()]
    }
  })
  expect(wrapper.text()).toContain(import.meta.env.VITE_APP_VERSION)
})
