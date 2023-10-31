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
    <pre class="header_title">{{ title }}</pre>
  </header>
  <c-menu :is-collapse="isCollapse" :active-path="activePath" :menus="menus" />
</template>

<script setup lang="ts">
  import { ref, reactive } from 'vue'
  import { useStore } from '@/pinia/layout'
  import { storeToRefs } from 'pinia'
  import { useRoute } from 'vue-router'
  import { MENUS } from './types'
  import cMenu from '@/components/menu/index.vue'

  const menus = reactive(MENUS)
  const store = useStore()
  const { isCollapse } = storeToRefs(store)
  const router = useRoute()
  const activePath = ref(router.path)
  /* eslint-disable */
  // define __APP_VERSION__ in vite.config.ts
  // @ts-ignore
  const title = `Apache Kyuubi\n${__APP_VERSION__}`
  /* eslint-enable */
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
      top: -12.5px;
      vertical-align: middle;
    }
    pre {
      display: inline-block;
      width: 160px;
      margin-top: 10px;
      line-height: 25px;
      text-align: center;
    }
  }
  .el-menu {
    margin-top: 64px;
  }
</style>
