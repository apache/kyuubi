{{/*
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/}}

{{/*
A comma separated string of enabled frontend protocols, e.g. "REST,THRIFT_BINARY".
For details, see 'kyuubi.frontend.protocols': https://kyuubi.readthedocs.io/en/master/configuration/settings.html#frontend
*/}}
{{- define "kyuubi.frontend.protocols" -}}
  {{- $protocols := list }}
  {{- range $name, $frontend := .Values.server }}
    {{- if $frontend.enabled }}
      {{- $protocols = $name | snakecase | upper | append $protocols }}
    {{- end }}
  {{- end }}
  {{- if not $protocols }}
    {{ fail "At least one frontend protocol must be enabled!" }}
  {{- end }}
  {{- $protocols |  join "," }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kyuubi.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "kyuubi.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "kyuubi.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
