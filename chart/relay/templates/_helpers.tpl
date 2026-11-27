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

{{/* The store: plugin when plugin.command or plugin.sidecar is set, else .Values.store, else temporal when temporal.address is set, else json (the relay's own rule). */}}
{{- define "relay.store" -}}
{{- $plugin := or .Values.plugin.command .Values.plugin.sidecar.enabled -}}
{{- if and .Values.plugin.command .Values.plugin.sidecar.enabled -}}
{{- fail "set plugin.command or plugin.sidecar.enabled, not both" -}}
{{- end -}}
{{- if and $plugin .Values.store -}}
{{- fail "set store or a plugin (plugin.command, plugin.sidecar), not both: a plugin is the relay's store" -}}
{{- end -}}
{{- $store := ternary "plugin" (.Values.store | default (ternary "temporal" "json" (not (empty .Values.temporal.address)))) (not (empty $plugin)) -}}
{{- if not (has $store (list "json" "temporal" "plugin")) -}}
{{- fail "store must be json or temporal" -}}
{{- end -}}
{{- $store -}}
{{- end -}}

{{- define "relay.claimName" -}}
{{- default (printf "%s-data" (include "relay.fullname" .)) .Values.persistence.existingClaim -}}
{{- end -}}


{{/* True when the pod has the data volume: the JSON store, or a plugin command in the image (it keeps its files in /data). */}}
{{- define "relay.dataVolume" -}}
{{- $store := include "relay.store" . -}}
{{- if or (eq $store "json") (and (eq $store "plugin") .Values.plugin.command) -}}true{{- end -}}
{{- end -}}
