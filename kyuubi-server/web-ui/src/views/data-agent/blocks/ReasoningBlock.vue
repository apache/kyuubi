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
  <div class="reasoning-block">
    <div class="reasoning-head" @click="emit('toggle')">
      <el-icon
        :size="11"
        class="reasoning-toggle"
        :class="{ open: block.expanded }">
        <ArrowRight />
      </el-icon>
      <el-icon :size="12" class="reasoning-icon"><MagicStick /></el-icon>
      <span class="reasoning-label">
        {{ thinking ? $t('data_agent.thinking') : $t('data_agent.thoughts') }}
      </span>
    </div>
    <div v-if="block.expanded" class="reasoning-body">{{ block.text }}</div>
  </div>
</template>

<script lang="ts" setup>
  import { ArrowRight, MagicStick } from '@element-plus/icons-vue'
  import type { ChatBlock } from '../types'

  defineProps<{
    block: ChatBlock
    thinking: boolean
  }>()

  const emit = defineEmits<{ (e: 'toggle'): void }>()
</script>

<style lang="scss" scoped>
  .reasoning-block {
    margin: 0;
    font-size: 12px;
    color: #909399;
  }
  .reasoning-head {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 14px;
    cursor: pointer;
    user-select: none;
    transition: color 0.15s;

    &:hover {
      color: #606266;
    }
  }
  .reasoning-toggle {
    transition: transform 0.15s ease;
    color: #c0c4cc;
    &.open {
      transform: rotate(90deg);
    }
  }
  .reasoning-icon {
    color: #b9a4f0;
  }
  .reasoning-label {
    font-weight: 500;
  }
  .reasoning-body {
    padding: 0 14px 8px 32px;
    font-size: 12px;
    line-height: 1.7;
    color: #909399;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 320px;
    overflow-y: auto;

    &::-webkit-scrollbar {
      width: 4px;
    }
    &::-webkit-scrollbar-thumb {
      background: #dcdfe6;
      border-radius: 2px;
    }
  }
</style>
