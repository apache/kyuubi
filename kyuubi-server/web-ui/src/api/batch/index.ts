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

import request from '@/utils/request'
import {
  IBatch,
  IBatchSearch,
  ICloseBatchResponse,
  IGetBatchesResponse,
  IOperationLog
} from './types'

function cleanParams(params: IBatchSearch): IBatchSearch {
  return Object.fromEntries(
    Object.entries(params).filter(([, value]) => value !== '')
  ) as IBatchSearch
}

export function getAllBatches(params: IBatchSearch) {
  return request({
    url: 'api/v1/batches',
    method: 'get',
    params: cleanParams(params)
  }) as Promise<IGetBatchesResponse>
}

export function getBatch(batchId: string) {
  return request({
    url: `api/v1/batches/${batchId}`,
    method: 'get'
  }) as Promise<IBatch>
}

export function deleteBatch(batchId: string) {
  return request({
    url: `api/v1/batches/${batchId}`,
    method: 'delete'
  }) as Promise<ICloseBatchResponse>
}

export function getBatchLocalLog(batchId: string, from = 0, size = 1000) {
  return request({
    url: `api/v1/batches/${batchId}/localLog`,
    method: 'get',
    params: {
      from,
      size
    }
  }) as Promise<IOperationLog>
}
