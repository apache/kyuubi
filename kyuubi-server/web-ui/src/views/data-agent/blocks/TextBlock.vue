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
  <div class="text-block">
    <MarkdownContent :text="text" :streaming="streaming" />
  </div>
</template>

<script lang="ts" setup>
  import MarkdownContent from './MarkdownContent.vue'

  defineProps<{
    text: string
    streaming: boolean
  }>()
</script>

<style lang="scss" scoped>
  .text-block {
    padding: 12px 16px;
    font-size: 14px;
    line-height: 1.7;
    overflow-x: hidden;

    .markdown-body {
      :deep(p) {
        margin: 0 0 8px;
        &:last-child {
          margin-bottom: 0;
        }
      }
      :deep(pre) {
        background: #1e1e2e;
        color: #cdd6f4;
        border-radius: 8px;
        padding: 14px;
        overflow-x: auto;
        font-size: 13px;
        margin: 8px 0;
        position: relative;
      }
      :deep(code) {
        font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo,
          monospace;
        font-size: 13px;
      }
      :deep(code:not(pre code)) {
        background: #f0f1f3;
        padding: 2px 6px;
        border-radius: 4px;
        color: #c7254e;
      }
      :deep(table) {
        display: block;
        max-width: 100%;
        overflow-x: auto;
        border-collapse: collapse;
        margin: 8px 0;
        border-radius: 6px;

        &::-webkit-scrollbar {
          height: 6px;
        }
        &::-webkit-scrollbar-thumb {
          background: #dcdfe6;
          border-radius: 3px;
        }
      }
      :deep(th),
      :deep(td) {
        border: 1px solid #e5e6eb;
        padding: 8px 12px;
        text-align: left;
        font-size: 13px;
        white-space: nowrap;
      }
      :deep(th) {
        background: #f7f8fa;
        font-weight: 600;
        color: #4e5969;
      }
      :deep(ul),
      :deep(ol) {
        padding-left: 20px;
        margin: 4px 0 8px;
      }
      :deep(li) {
        margin: 2px 0;
      }
    }
  }
</style>
