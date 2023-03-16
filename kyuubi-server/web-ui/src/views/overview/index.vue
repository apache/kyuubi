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
        <c-card :title="card.title" :value="card.value" />
      </el-col>
    </el-row>
  </main>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import cCard from '@/components/card/index.vue'

  const cards = reactive([
    {
      title: 'Opened Session',
      value: 1
    },
    {
      title: 'ExecPool Size',
      value: 2
    },
    {
      title: 'ExecPool ActiveCount',
      value: 3
    }
  ])
</script>

<style scoped></style>
