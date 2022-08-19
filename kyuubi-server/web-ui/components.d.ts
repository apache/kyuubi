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

import '@vue/runtime-core'

export {}

declare module '@vue/runtime-core' {
  export interface GlobalComponents {
    Card: typeof import('./src/components/card/index.vue')['default']
    ElAside: typeof import('element-plus/es')['ElAside']
    ElButton: typeof import('element-plus/es')['ElButton']
    ElCard: typeof import('element-plus/es')['ElCard']
    ElCol: typeof import('element-plus/es')['ElCol']
    ElContainer: typeof import('element-plus/es')['ElContainer']
    ElDropdown: typeof import('element-plus/es')['ElDropdown']
    ElDropdownItem: typeof import('element-plus/es')['ElDropdownItem']
    ElDropdownMenu: typeof import('element-plus/es')['ElDropdownMenu']
    ElHeader: typeof import('element-plus/es')['ElHeader']
    ElIcon: typeof import('element-plus/es')['ElIcon']
    ElMain: typeof import('element-plus/es')['ElMain']
    ElMenu: typeof import('element-plus/es')['ElMenu']
    ElMenuItem: typeof import('element-plus/es')['ElMenuItem']
    ElMenuItemGroup: typeof import('element-plus/es')['ElMenuItemGroup']
    ElRow: typeof import('element-plus/es')['ElRow']
    ElSubMenu: typeof import('element-plus/es')['ElSubMenu']
    Menu: typeof import('./src/components/menu/index.vue')['default']
    RouterLink: typeof import('vue-router')['RouterLink']
    RouterView: typeof import('vue-router')['RouterView']
  }
}
