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

import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useLocalesStore } from '@/pinia/locales/locales'
import type { Locale } from '@/pinia/locales/types'
import { LOCALES } from './types'

export function useLocales() {
  const { locale } = useI18n()
  const localesStore = useLocalesStore()

  const currentLocale = ref(
    LOCALES.find((item: { key: string }) => item.key === localesStore.getLocale)
      ?.label || 'English'
  )

  const changeLocale = (key: string | number) => {
    localesStore.setLocale((locale.value = key as Locale))
    currentLocale.value = LOCALES.find(
      (item: { key: string }) => item.key === key
    )?.label as string
  }
  return {
    changeLocale,
    currentLocale
  }
}
