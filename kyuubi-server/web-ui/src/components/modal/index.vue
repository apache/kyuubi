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
  <el-dialog
    v-model="visible"
    :align-center="true"
    :title="title"
    :before-close="handleCancel"
    class="modal-container"
  >
    <slot></slot>
    <template #footer>
      <span class="dialog-footer">
        <el-button
          v-if="cancelShow"
          :clsss="confirmClassName"
          @click="handleCancel"
          >{{ cancelText || $t('cancel') }}</el-button
        >
        <el-button
          type="primary"
          :class="cancelClassName"
          :disabled="confirmDisabled"
          :loading="confirmLoading"
          @click="handleConfirm"
        >
          {{ confirmText || $t('confirm') }}
        </el-button>
      </span>
    </template>
  </el-dialog>
</template>

<script lang="ts" setup>
  import { PropType, computed } from 'vue'

  const props = defineProps({
    show: {
      type: Boolean as PropType<boolean>,
      default: false
    },
    title: {
      type: String as PropType<string>,
      required: true
    },
    cancelText: {
      type: String as PropType<string>,
      default: null
    },
    cancelShow: {
      type: Boolean as PropType<boolean>,
      default: true
    },
    confirmText: {
      type: String as PropType<string>,
      default: null
    },
    confirmClassName: {
      type: String as PropType<string>,
      default: ''
    },
    cancelClassName: {
      type: String as PropType<string>,
      default: ''
    },
    confirmDisabled: {
      type: Boolean as PropType<boolean>,
      default: false
    },
    confirmLoading: {
      type: Boolean as PropType<boolean>,
      default: false
    }
  })

  const emit = defineEmits(['cancel', 'confirm'])

  const visible = computed(() => props.show)

  const handleCancel = () => {
    emit('cancel')
  }

  const handleConfirm = () => {
    emit('confirm')
  }
</script>

<style lang="scss">
  .modal-container {
    .el-dialog__body {
      max-height: 60vh;
      overflow: auto;
    }
  }
</style>
