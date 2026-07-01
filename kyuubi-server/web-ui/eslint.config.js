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

import js from '@eslint/js'
import pluginVue from 'eslint-plugin-vue'
import {
  defineConfigWithVueTs,
  vueTsConfigs
} from '@vue/eslint-config-typescript'
import prettierRecommended from 'eslint-plugin-prettier/recommended'

// Flat config (ESLint 9). Formatting is delegated to Prettier via
// eslint-plugin-prettier, so pure style rules are intentionally left to it.
export default defineConfigWithVueTs(
  {
    name: 'kyuubi/files',
    files: ['**/*.{ts,mts,tsx,vue}']
  },
  {
    name: 'kyuubi/ignores',
    ignores: ['dist/**', 'coverage/**', 'node/**']
  },
  js.configs.recommended,
  pluginVue.configs['flat/recommended'],
  vueTsConfigs.recommended,
  prettierRecommended,
  {
    name: 'kyuubi/rules',
    rules: {
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-empty-function': 'off',
      // typescript-eslint 8 split the former `ban-types`/`no-empty-interface`
      // into these; keep them off to preserve the previous rule set.
      '@typescript-eslint/no-empty-object-type': 'off',
      '@typescript-eslint/no-unsafe-function-type': 'off',
      '@typescript-eslint/no-wrapper-object-types': 'off',
      '@typescript-eslint/ban-ts-comment': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-use-before-define': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^(unused|ignored).*$',
          varsIgnorePattern: '^(unused|ignored).*$'
        }
      ],
      'no-use-before-define': 'off',
      'no-console': 'error',
      'vue/custom-event-name-casing': 'off',
      'vue/multi-word-component-names': 'off',
      'vue/component-definition-name-casing': 'off',
      'vue/require-valid-default-prop': 'off',
      'vue/no-required-prop-with-default': 'off',
      'vue/no-setup-props-reactivity-loss': 'off'
    }
  }
)
