<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

<template>
  <div class="header-container">
    <div class="left-container">
      <el-icon :size="20" @click="_changeCollapse">
        <component :is="isCollapse ? 'Expand' : 'Fold'" />
      </el-icon>
    </div>
    <div class="right-container">
      <template v-if="authStore.isAuthenticated">
        <el-dropdown>
          <span class="el-dropdown-link">
            {{ authStore.user }}
            <el-icon class="el-icon--right">
              <arrow-down />
            </el-icon>
          </span>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="handleLogout"
                >Sign out</el-dropdown-item
              >
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </template>
      <el-button v-else @click="showLoginModal">Sign in</el-button>
      <el-dropdown @command="handleClick">
        <span class="el-dropdown-link">
          {{ currentLocale }}
          <el-icon class="el-icon--right">
            <arrow-down />
          </el-icon>
        </span>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item
              v-for="(locale, key) in locales"
              :key="key"
              :command="locale.key">
              {{ locale.label }}
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>
  </div>
</template>

<script lang="ts" setup>
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useLocales } from './use-locales'
  import { LOCALES } from './types'
  import { reactive } from 'vue'
  import { useAuthStore } from '@/pinia/auth/auth'

  const locales = reactive(LOCALES)
  const { changeLocale, currentLocale } = useLocales()
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const { changeCollapse } = store

  function _changeCollapse() {
    changeCollapse()
  }

  function handleClick(command: string) {
    changeLocale(command)
  }

  const authStore = useAuthStore()
  const handleLogout = () => {
    logout()
  }
  const logout = () => {
    authStore.clearUser()
  }

  const showLoginModal = () => {
    window.dispatchEvent(new CustomEvent('auth-required'))
  }
</script>

<style lang="scss" scoped>
  .header-container {
    display: flex;
    justify-content: space-between;
    width: 100%;
  }

  .left-container {
    > .el-icon {
      padding: 0 24px;
      cursor: pointer;
      position: relative;
      top: 2px;
    }
  }

  .right-container {
    display: flex;
    align-items: center;

    > *:not(:last-child) {
      margin-right: 16px;
    }

    > .el-dropdown .el-icon,
    > .el-button {
      position: relative;
      top: 2px;
    }
  }
</style>
