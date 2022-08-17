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
import Menu from '@/components/menu/index.vue'
import { expect, test } from 'vitest'

test('mount component', () => {
  const wrapper = mount(Menu, {
    props: {
      isCollapse: true,
      menus: [
        {
          label: 'Overview',
          icon: 'Odometer',
          router: '/overview'
        },
        {
          label: 'Workload',
          icon: 'List',
          children: [
            {
              label: 'Analysis',
              icon: 'VideoPlay',
              router: '/workload/analysis'
            },
            {
              label: 'Queue',
              icon: 'Select',
              router: '/workload/queue'
            },
            {
              label: 'Session',
              icon: 'Select',
              router: '/workload/session'
            },
            {
              label: 'Query',
              icon: 'Select',
              router: '/workload/query'
            }
          ]
        }
      ],
      activePath: '/overview'
    }
  })

  expect(wrapper.findAll('el-menu-item')).toHaveLength(5)
  expect(wrapper.findAll('el-sub-menu')).toHaveLength(1)
})
