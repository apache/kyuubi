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
  <div ref="codeEditBox" style="height: 100%" />
</template>

<script lang="ts" setup>
  import * as monaco from 'monaco-editor'
  import { format } from 'sql-formatter'
  import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker'
  import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
  import { editorProps } from './type'
  import { useEditorStore } from '@/pinia/editor'
  import { ref, toRaw, watch, onBeforeUnmount, onMounted } from 'vue'

  // @ts-ignore: worker
  self.MonacoEnvironment = {
    getWorker(_: string, label: string) {
      if (label === 'json') {
        return new jsonWorker()
      }
      return new EditorWorker()
    }
  }

  const props = defineProps(editorProps)
  const emit = defineEmits([
    'update:modelValue',
    'change',
    'editorMounted',
    'editorSave'
  ])

  const editorStore = useEditorStore()
  const monacoEditorThemeRef = ref(
    editorStore.getCurrentTheme === 'dark' ? 'vs-dark' : 'vs'
  )
  let editor: monaco.editor.IStandaloneCodeEditor
  const codeEditBox = ref()
  const init = () => {
    monaco.languages.typescript.javascriptDefaults.setDiagnosticsOptions({
      noSemanticValidation: true,
      noSyntaxValidation: false
    })
    monaco.languages.typescript.javascriptDefaults.setCompilerOptions({
      target: monaco.languages.typescript.ScriptTarget.ES2020,
      allowNonTsExtensions: true
    })
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: function (model: any, position: any) {
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn
        }
        const suggestions = []
        const sqlStr = [
          'select',
          'from',
          'where',
          'and',
          'or',
          'limit',
          'order by',
          'group by'
        ]
        for (const i in sqlStr) {
          suggestions.push({
            label: sqlStr[i],
            kind: monaco.languages.CompletionItemKind['Function'],
            insertText: sqlStr[i],
            detail: '',
            range: range
          })
        }
        return {
          suggestions: suggestions
        }
      }
    })

    editor = monaco.editor.create(codeEditBox.value, {
      value: props.modelValue,
      language: props.language,
      theme: monacoEditorThemeRef.value,
      ...props.options
    })

    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, function () {
      emit('editorSave')
    })

    editor.setValue(format(toRaw(editor).getValue()))

    editor.onDidChangeModelContent(() => {
      const value = editor.getValue()
      emit('update:modelValue', value)
      emit('change', value)
    })
    emit('editorMounted', editor)
  }
  watch(
    () => props.modelValue,
    (newValue) => {
      if (editor) {
        const value = editor.getValue()
        if (newValue !== value) {
          editor.setValue(newValue)
          editor.setValue(format(toRaw(editor).getValue()))
        }
      }
    }
  )
  watch(
    () => props.options,
    (newValue) => {
      editor.updateOptions(newValue)
    },
    { deep: true }
  )
  watch(
    () => props.language,
    (newValue) => {
      monaco.editor.setModelLanguage(editor.getModel()!, newValue)
    }
  )
  watch(
    () => editorStore.getCurrentTheme,
    () => {
      editor?.dispose()
      monacoEditorThemeRef.value =
        editorStore.getCurrentTheme === 'dark' ? 'vs-dark' : 'vs'
      init()
    }
  )

  onBeforeUnmount(() => {
    editor.dispose()
  })
  onMounted(() => {
    init()
  })
</script>
