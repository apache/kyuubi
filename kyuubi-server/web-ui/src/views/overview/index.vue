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
  <main>
    <el-row :gutter="12">
      <el-col v-for="(card, index) in cards" :key="index" :md="8">
        <c-card
          v-loading="card.loading"
          :title="card.title"
          :value="card.value == null || card.value === '' ? '-' : card.value"
        />
      </el-col>
    </el-row>
  </main>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import { getSessionsCount, getSessionsExecPoolData } from '@/api/overview'
  import cCard from '@/components/card/index.vue'

  const cards = reactive([
    {
      title: 'Opened Session',
      value: '',
      loading: true
    },
    {
      title: 'ExecPool Size',
      value: '',
      loading: true
    },
    {
      title: 'ExecPool ActiveCount',
      value: '',
      loading: true
    }
  ])

  const getOpenedSession = () => {
    cards[0].loading = true
    getSessionsCount()
      .then((res: any) => {
        cards[0].value = res?.openSessionCount
      })
      .finally(() => {
        cards[0].loading = false
      })
  }

  const getSessionsExecPool = () => {
    getSessionsExecPoolData()
      .then((res: any) => {
        cards[1].value = res?.execPoolSize
        cards[2].value = res?.execPoolActiveCount
      })
      .finally(() => {
        cards[1].loading = false
        cards[2].loading = false
      })
  }

  const init = () => {
    getOpenedSession()
    getSessionsExecPool()
  }

  init()
</script>

<style scoped></style>
