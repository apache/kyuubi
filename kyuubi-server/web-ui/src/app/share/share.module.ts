/*
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
 */

import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageModule } from 'ng-zorro-antd/message';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { FileReadDirective } from 'share/common/file-read/file-read.directive';
import { NavigationComponent } from 'share/common/navigation/navigation.component';
import { ResizeComponent } from 'share/common/resize/resize.component';
import { JobBadgeComponent } from 'share/customize/job-badge/job-badge.component';
import { JobChartComponent } from 'share/customize/job-chart/job-chart.component';
import { RefreshDownloadComponent } from 'share/customize/refresh-download/refresh-download.component';
import { PipeModule } from 'share/pipes/pipe.module';

import { AutoResizeDirective } from './common/editor/auto-resize.directive';

@NgModule({
  imports: [
    CommonModule,
    PipeModule,
    NzCardModule,
    NzTableModule,
    NzButtonModule,
    NzDividerModule,
    NzToolTipModule,
    NzMessageModule,
    NzTabsModule,
    NzIconModule
  ],
  declarations: [
    JobBadgeComponent,
    FileReadDirective,
    NavigationComponent,
    RefreshDownloadComponent,
    ResizeComponent,
    JobChartComponent,
    AutoResizeDirective
  ],
  exports: [
    PipeModule,
    FileReadDirective,
    NavigationComponent,
    RefreshDownloadComponent,
    JobBadgeComponent,
    ResizeComponent,
    JobChartComponent,
    AutoResizeDirective
  ]
})
export class ShareModule {}
