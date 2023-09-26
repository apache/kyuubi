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

export const MENUS = [
  {
    label: 'Overview',
    icon: 'Odometer',
    router: '/overview'
  },
  {
    label: 'Management',
    icon: 'List',
    children: [
      {
        label: 'Session',
        router: '/management/session'
      },
      {
        label: 'Operation',
        router: '/management/operation'
      },
      {
        label: 'Engine',
        router: '/management/engine'
      },
      {
        label: 'Server',
        router: '/management/server'
      }
    ]
  },
  {
    label: 'Workload',
    icon: 'List',
    children: [
      {
        label: 'Analysis',
        icon: 'VideoPlay',
        router: '/workload/analysis'
      },
      {
        label: 'Queue',
        icon: 'Select',
        router: '/workload/queue'
      },
      {
        label: 'Session',
        icon: 'Select',
        router: '/workload/session'
      },
      {
        label: 'Query',
        icon: 'Select',
        router: '/workload/query'
      }
    ]
  },
  {
    label: 'Operation',
    icon: 'List',
    children: [
      {
        label: 'Running Jobs',
        icon: 'VideoPlay',
        router: '/operation/runningJobs'
      },
      {
        label: 'Completed Jobs',
        icon: 'Select',
        router: '/operation/completedJobs'
      }
    ]
  },
  {
    label: 'Swagger',
    icon: 'List',
    router: '/swagger'
  },
  {
    label: 'Contact Us',
    icon: 'PhoneFilled',
    router: '/contact'
  }
]
