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
import Engine from '@/views/management/engine/index.vue'
import { shallowMount } from '@vue/test-utils'
import { createI18n } from 'vue-i18n'
import { getStore } from '@/test/unit/utils'
import { createRouter, createWebHistory } from 'vue-router'
import ElementPlus from 'element-plus'
import { expect, test } from 'vitest'

test('proxy ui', async () => {
  expect(Engine).toBeTruthy()
  const i18n = createI18n({
    legacy: false,
    globalInjection: true
  })

  const mockRouter = createRouter({ history: createWebHistory(), routes: [] })
  mockRouter.currentRoute.value.params = {
    path: '/management/engine'
  }

  const wrapper = shallowMount(Engine, {
    global: {
      plugins: [i18n, mockRouter, getStore(), ElementPlus]
    }
  })
  expect(wrapper.vm.getProxyEngineUI('host:ip')).toEqual(
    `${import.meta.env.VITE_APP_DEV_WEB_URL}engine-ui/host:ip/`
  )
})
