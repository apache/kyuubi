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

<!-- v-html is allowed only for sanitized markdown from renderMarkdown/useStreamingMarkdown. -->
<template>
  <!-- eslint-disable-next-line vue/no-v-html -->
  <div class="markdown-body" v-html="html"></div>
</template>

<script lang="ts" setup>
  import { computed, toRef } from 'vue'
  import { renderMarkdown } from '../utils/markdown'
  import { useStreamingMarkdown } from '../composables/useStreamingMarkdown'

  const props = defineProps<{
    text: string
    streaming?: boolean
  }>()

  const textRef = toRef(props, 'text')
  const streamingHtml = useStreamingMarkdown(
    () => textRef.value,
    () => !!props.streaming
  )
  const html = computed(() =>
    props.streaming ? streamingHtml.value : renderMarkdown(props.text)
  )
</script>
