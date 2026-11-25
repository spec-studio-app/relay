{{- define "relay.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "relay.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "relay.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "relay.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "relay.labels" -}}
app.kubernetes.io/name: {{ include "relay.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "relay.selectorLabels" -}}
app.kubernetes.io/name: {{ include "relay.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "relay.secretName" -}}
{{- default (include "relay.fullname" .) .Values.existingSecret -}}
{{- end -}}

{{/* An env entry from the Secret, optional unless required. */}}
{{- define "relay.secretEnv" -}}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ .secret }}
      key: {{ .key }}
      optional: {{ not .required }}
{{- end -}}

{{/* The store: .Values.store, else temporal when temporal.address is set, else json (the relay's own rule). */}}
{{- define "relay.store" -}}
{{- $store := .Values.store | default (ternary "temporal" "json" (not (empty .Values.temporal.address))) -}}
{{- if not (has $store (list "json" "temporal")) -}}
{{- fail "store must be json or temporal" -}}
{{- end -}}
{{- $store -}}
{{- end -}}

{{- define "relay.claimName" -}}
{{- default (printf "%s-data" (include "relay.fullname" .)) .Values.persistence.existingClaim -}}
{{- end -}}
