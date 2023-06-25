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
  <div class="common-layout">
    <el-container>
      <el-aside>
        <Aside />
      </el-aside>
      <el-container>
        <el-header>
          <Header />
        </el-header>
        <el-main>
          <router-view />
        </el-main>
      </el-container>
    </el-container>
  </div>
</template>

<script setup lang="ts">
  import { onMounted } from 'vue'
  import Aside from './components/aside/index.vue'
  import Header from './components/header/index.vue'
  import { useLocalesStore } from '@/pinia/locales/locales'
  import { useI18n } from 'vue-i18n'

  const { locale } = useI18n()
  const localesStore = useLocalesStore()

  onMounted(() => {
    locale.value = localesStore.getLocale
  })
</script>

<style lang="scss" scoped>
  .common-layout {
    height: 100%;

    .el-container {
      min-height: 100vh;

      ::v-deep(.el-aside) {
        width: auto;
        position: relative;
        background: #001529;
      }

      .el-header {
        display: flex;
        align-items: center;
        height: 64px;
        padding: 0 12px 0 0;
        border-bottom: 1px solid #f0f0f0;
        background: #fff;
        box-shadow: 0 1px 4px rgb(0 21 41 / 8%);
      }
    }
  }
</style>
