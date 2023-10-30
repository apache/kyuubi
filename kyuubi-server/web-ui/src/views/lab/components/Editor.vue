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
    <MonacoEditor
      v-model="editorVariables.content"
      :language="editorVariables.language"
      @editor-mounted="editorMounted"
      @change="handleContentChange"
      @editor-save="editorSave" />
    <el-space class="search-box" style="margin-top: 8px">
      <el-select
        v-model="param.engineType"
        :placeholder="$t('engine_type')"
        style="width: 210px">
        <el-option
          v-for="item in getEngineType()"
          :key="item"
          :label="item"
          :value="item" />
      </el-select>
      <el-select
        v-model="param.server"
        :placeholder="'Server IP'"
        style="width: 210px">
        <el-option
          v-for="item in serverList"
          :key="item.instance"
          :label="item.host"
          :value="item.instance" />
      </el-select>
      <el-button type="primary" icon="Search" @click="handleQuerySql" />
    </el-space>
  </div>
</template>

<script lang="ts" setup>
  import MonacoEditor from '@/components/monaco-editor/index.vue'
  import { ref, reactive, toRaw } from 'vue'
  import type { Ref } from 'vue'
  import * as monaco from 'monaco-editor'
  import { format } from 'sql-formatter'
  import { getEngineType } from '@/utils/engine'
  import { openSession, runSql, getSqlRowset } from '@/api/sql'
  import { getAllServer } from '@/api/server'
  import type { IServer } from '@/api/server/types'
  import { IResponse } from './types'

  const param = reactive({
    engineType: 'SPARK_SQL',
    server: ''
  })
  const serverList: Ref<IServer[]> = ref([])

  const editorVariables = reactive({
    editor: {} as any,
    language: 'sql',
    content: ''
  })

  const editorMounted = (editor: monaco.editor.IStandaloneCodeEditor) => {
    editorVariables.editor = editor
  }
  const handleFormat = () => {
    toRaw(editorVariables.editor).setValue(
      format(toRaw(editorVariables.editor).getValue())
    )
  }

  const editorSave = () => {
    handleFormat()
  }

  const handleContentChange = (value: string) => {
    editorVariables.content = value
  }

  const handleQuerySql = async () => {
    const openSessionResult: IResponse = await openSession(param.server, {
      'kyuubi.engine.type': param.engineType
    }).catch(() => {
      // 报错提醒
    })
    if (!openSessionResult) return

    const runSqlResult: IResponse = await runSql(
      {
        statement: editorVariables.content,
        runAsync: false
      },
      openSessionResult.identifier
    ).catch(() => {
      // 报错提醒
    })
    if (!runSqlResult) return

    Promise.all([
      getSqlRowset({
        operationHandleStr: runSqlResult.identifier,
        fetchorientation: 'FETCH_NEXT'
      })
    ])

    // const sqlResult = await getSqlResult({
    //   operationHandleStr: runSqlResult.identifier,
    //   fetchorientation: 'FETCH_NEXT'
    // }).then((res: any) => {
    //   console.log(res)
    // })
  }

  const getServerList = async () => {
    serverList.value = (await getAllServer().catch(() => [])) || []
  }

  getServerList()
</script>

<style lang="scss" scoped>
  .container {
    height: 70%;
  }
</style>
