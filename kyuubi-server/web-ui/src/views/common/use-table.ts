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

import { ref, Ref } from 'vue'

export function useTable() {
  const tableData: Ref<any[]> = ref([])
  const loading = ref(false)
  const searchParam = ref()

  const getList = (func: Function, data?: any) => {
    loading.value = true
    func(data)
      .then((res: any[]) => {
        tableData.value = res || []
      })
      .catch(() => (tableData.value = []))
      .finally(() => {
        loading.value = false
      })
  }

  return {
    tableData,
    loading,
    searchParam,
    getList
  }
}
