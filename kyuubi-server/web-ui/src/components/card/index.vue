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
  <el-card :shadow="shadow">
    <header>{{ title }}</header>
    <main>{{ value }}</main>
  </el-card>
</template>

<script lang="ts">
  export default {
    name: 'CardIndex',
    props: {
      title: {
        type: [String, Number],
        required: true
      },
      value: {
        type: [String, Number, Boolean, Date],
        required: true
      },
      shadow: {
        type: String,
        default: 'always'
      }
    }
  }
</script>

<style lang="scss" scoped>
  main {
    color: #52c41a;
    font-size: 36px;
    line-height: 38px;
  }
</style>
