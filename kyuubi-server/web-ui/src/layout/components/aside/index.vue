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
    <img v-if="!isCollapse" src="@/assets/images/kyuubi-logo.svg" />
    <img v-else class="collapsed-logo" src="@/assets/images/kyuubi.png" />
    <pre v-if="!isCollapse">{{ version }}</pre>
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
  const version = import.meta.env.VITE_APP_VERSION
</script>

<style lang="scss" scoped>
  $height: 64px;
  header {
    width: 100%;
    position: absolute;
    top: 0;
    left: 0;
    height: $height;
    line-height: $height;
    padding: 0 16px;
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    box-sizing: border-box;
    img {
      width: 140px;
      height: 50px;
      &.collapsed-logo {
        width: 40px;
        height: 40px;
        position: relative;
        top: -4px;
        left: -4px;
      }
    }
    span {
      position: relative;
      top: 17px;
      font-size: 10px;
      font-family: 'Myriad Pro', 'Helvetica Neue', Arial, Helvetica, sans-serif;
      color: rgba(255, 255, 255, 0.87);
    }
  }
  .el-menu {
    margin-top: $height;
  }
</style>
