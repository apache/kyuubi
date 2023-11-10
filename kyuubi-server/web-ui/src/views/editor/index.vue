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
  <div class="container">
    <el-tabs
      v-model="editableTabsValue"
      type="border-card"
      class="editor-el-tabs"
      editable
      @edit="handleTabsEdit">
      <el-tab-pane
        v-for="item in editableTabs"
        :key="item.name"
        :label="item.title"
        :name="item.name">
        <Editor />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script lang="ts" setup>
  import Editor from './components/Editor.vue'
  import { ref } from 'vue'
  import { TabPanelName } from 'element-plus'

  const editableTabsValue = ref('1')
  const editableTabs = ref([
    {
      title: 'Session 1',
      name: '1'
    }
  ])

  const handleTabsEdit = (
    targetName: TabPanelName | undefined,
    action: 'remove' | 'add'
  ) => {
    if (action === 'add') {
      const tabLength = editableTabs.value.length + 1
      const newTabName = `${tabLength}`
      editableTabs.value.push({
        title: `Session ${tabLength}`,
        name: newTabName
      })
      editableTabsValue.value = newTabName
    } else if (action === 'remove') {
      const tabs = editableTabs.value
      let activeName = editableTabsValue.value
      if (activeName === targetName) {
        tabs.forEach((tab, index) => {
          if (tab.name === targetName) {
            const nextTab = tabs[index + 1] || tabs[index - 1]
            if (nextTab) {
              activeName = nextTab.name
            }
          }
        })
      }

      editableTabsValue.value = activeName
      editableTabs.value = tabs.filter((tab) => tab.name !== targetName)
    }
  }
</script>

<style lang="scss" scoped>
  .container {
    height: calc(100vh - 66px);
    margin: -20px;
  }

  :deep(.editor-el-tabs) {
    height: 100%;
    overflow: auto;
    .el-tabs__header {
      display: flex;
      flex-direction: row-reverse;
      justify-content: flex-end;
      background: #f5f5f5;
    }
    .el-tabs__content {
      overflow: auto;
      padding: 12px;
      .el-tab-pane {
        height: 100%;
      }
    }
  }

  :deep(.result-el-tabs) {
    .el-tabs__header {
      margin-bottom: 0;
      background: transparent;
      .el-tabs__nav-wrap {
        border-bottom: 1px solid #e4e7ed;
      }
      .el-tabs__nav {
        background: #f5f5f5;
      }
    }
    .el-tabs__content {
      padding: 10px 0 0 0;
      border: 1px solid #e4e7ed;
      border-top: none;
    }
  }
</style>
