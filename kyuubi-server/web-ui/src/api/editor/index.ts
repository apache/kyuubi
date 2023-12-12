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
import type {
  IOpenSessionRequest,
  IRunSqlRequest,
  IGetSqlRowsetRequest,
  IGetSqlMetadataRequest
} from './types'

export function openSession(data: IOpenSessionRequest): any {
  return request({
    url: 'api/v1/sessions',
    method: 'post',
    data
  })
}

export function closeSession(identifier: string): any {
  return request({
    url: `api/v1/sessions/${identifier}`,
    method: 'delete'
  })
}

export function runSql(data: IRunSqlRequest, identifier: string): any {
  return request({
    url: `api/v1/sessions/${identifier}/operations/statement`,
    method: 'post',
    data
  })
}

export function getSqlRowset(params: IGetSqlRowsetRequest): any {
  return request({
    url: `api/v1/operations/${params.operationHandleStr}/rowset`,
    method: 'get',
    params
  })
}

export function getSqlMetadata(params: IGetSqlMetadataRequest): any {
  return request({
    url: `api/v1/operations/${params.operationHandleStr}/resultsetmetadata`,
    method: 'get',
    params
  })
}

export function getLog(identifier: string): any {
  return request({
    url: `api/v1/operations/${identifier}/log`,
    method: 'get'
  })
}

export function closeOperation(identifier: string) {
  return request({
    url: `api/v1/admin/operations/${identifier}`,
    method: 'delete'
  })
}
