/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { PropType } from 'vue'

export type Theme = 'vs' | 'vs-dark'
export type FoldingStrategy = 'auto' | 'indentation'
export type RenderLineHighlight = 'all' | 'line' | 'none' | 'gutter'
export interface Options {
  automaticLayout?: boolean
  foldingStrategy?: FoldingStrategy
  renderLineHighlight?: RenderLineHighlight
  selectOnLineNumbers?: boolean
  minimap?: {
    enabled: boolean
  }
  readOnly: boolean
  contextmenu: boolean
  fontSize?: number
  scrollBeyondLastLine?: boolean
  overviewRulerBorder?: boolean
}

export const editorProps = {
  modelValue: {
    type: String as PropType<string>,
    default: null
  },
  width: {
    type: [String, Number] as PropType<string | number>,
    default: '100%'
  },
  height: {
    type: [String, Number] as PropType<string | number>,
    default: '100%'
  },
  language: {
    type: String as PropType<string>,
    default: 'sql'
  },
  theme: {
    type: String as PropType<any>,
    default: 'vs'
  },
  options: {
    type: Object as PropType<Options>,
    default() {
      return {
        automaticLayout: true,
        foldingStrategy: 'indentation',
        renderLineHighlight: 'line',
        selectOnLineNumbers: true,
        minimap: {
          enabled: false
        },
        readOnly: false,
        contextmenu: true,
        fontSize: 14,
        scrollBeyondLastLine: true,
        overviewRulerBorder: false
      }
    }
  }
}
