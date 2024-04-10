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

import { defineStore } from 'pinia'
import request from '@/utils/request'

export const useAuthStore = defineStore('auth', {
  state: () => ({
    user: null as string | null,
    authToken: null as string | null,
    isAuthenticated: false
  }),
  actions: {
    async setUser(user: string, password: string) {
      const response = await request({
        url: 'api/v1/ping',
        method: 'get',
        auth: {
          username: user,
          password: password
        }
      })

      if (response) {
        this.user = user
        this.authToken = `Basic ${btoa(user + ':' + password)}`
        this.isAuthenticated = true
      } else {
        throw new Error('Authentication failed')
      }
    },
    clearUser() {
      this.user = null
      this.authToken = null
      this.isAuthenticated = false
    }
  },
  persist: {
    key: 'auth'
  }
})
