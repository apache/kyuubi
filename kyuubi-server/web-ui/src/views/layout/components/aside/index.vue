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
  <header>
    <img src="@/assets/kyuubi.png" />
    <article v-if="!isCollapse">Apache Kyuubi Dashboard</article>
  </header>
  <c-menu :is-collapse="isCollapse" :active-path="activePath" :menus="menus" />
</template>

<script setup lang="ts">
  import { ref, watch, computed } from 'vue'
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useRoute } from 'vue-router'
  import cMenu from '@/components/menu/index.vue'
  import { useI18n } from 'vue-i18n'
  const { t, locale } = useI18n()

  const menus: any = ref([])
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const router = useRoute()
  const activeMenuMap: any = {
    '/session/sql-statistics': '/session/session-statistics'
  }
  const activePath = computed(() => {
    return activeMenuMap[router.path] || router.path
  })

  const initMenus = () => {
    menus.value = [
      {
        label: t('overview'),
        icon: 'Odometer',
        router: '/overview'
      },
      {
        label: t('session_management'),
        icon: 'List',
        children: [
          {
            label: t('session_statistics'),
            router: '/session/session-statistics'
          },
          {
            label: t('operation'),
            router: '/session/operation'
          }
        ]
      },
      {
        label: t('server_management'),
        icon: 'Coin',
        children: [
          {
            label: t('kyuubi_server_management'),
            router: '/server/kyuubi-service'
          },
          {
            label: t('engine_management'),
            router: '/server/engine'
          }
        ]
      },
      {
        label: t('run_sql'),
        icon: 'VideoPlay',
        router: '/run-sql'
      }
    ]
  }

  watch(locale, () => {
    initMenus()
  })
  initMenus()
</script>

<style lang="scss" scoped>
  header {
    position: absolute;
    top: 0;
    left: 0;
    height: 64px;
    padding-left: 16px;
    line-height: 64px;
    img {
      display: inline-block;
      width: 32px;
      height: 32px;
      position: relative;
      top: -4px;
      vertical-align: middle;
    }
    article {
      display: inline-block;
      margin-left: 12px;
      color: #fff;
      font-weight: 600;
      font-size: 14px;
      font-family: 'Myriad Pro', 'Helvetica Neue', Arial, Helvetica, sans-serif;
    }
  }
  .el-menu {
    margin-top: 64px;
  }
</style>
