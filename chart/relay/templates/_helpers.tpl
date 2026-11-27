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

{{/* The plugin values with their defaults, as YAML: a values file of an older chart has no plugin key (helm upgrade --reuse-values). */}}
{{- define "relay.plugin" -}}
{{- $defaults := dict "command" "" "url" "" "settings" (dict) "sidecar" (dict "enabled" false "image" "" "pullPolicy" "IfNotPresent" "port" 7070 "args" (list) "env" (list) "resources" (dict) "dataPath" "") -}}
{{- $given := deepCopy (.Values.plugin | default dict) -}}
{{- $sidecar := mergeOverwrite (deepCopy $defaults.sidecar) ($given.sidecar | default dict) -}}
{{- $out := mergeOverwrite (deepCopy $defaults) $given -}}
{{- $_ := set $out "sidecar" $sidecar -}}
{{- toYaml $out -}}
{{- end -}}

{{/* The store: plugin when plugin.command, plugin.url or plugin.sidecar is set, else .Values.store, else temporal when temporal.address is set, else json (the relay's own rule). */}}
{{- define "relay.store" -}}
{{- $p := fromYaml (include "relay.plugin" .) -}}
{{- $set := list -}}
{{- if $p.command }}{{ $set = append $set "plugin.command" }}{{ end -}}
{{- if $p.url }}{{ $set = append $set "plugin.url" }}{{ end -}}
{{- if $p.sidecar.enabled }}{{ $set = append $set "plugin.sidecar.enabled" }}{{ end -}}
{{- if gt (len $set) 1 -}}
{{- fail (printf "set one of plugin.command, plugin.url and plugin.sidecar.enabled, not %s" (join " and " $set)) -}}
{{- end -}}
{{- if and $set .Values.store -}}
{{- fail "set store or a plugin (plugin.command, plugin.url, plugin.sidecar), not both: a plugin is the relay's store" -}}
{{- end -}}
{{- $store := ternary "plugin" (.Values.store | default (ternary "temporal" "json" (not (empty .Values.temporal.address)))) (gt (len $set) 0) -}}
{{- if and (eq .Values.store "plugin") -}}
{{- fail "store must be json or temporal. For a plugin, set plugin.command, plugin.url or plugin.sidecar.enabled, and leave store empty" -}}
{{- end -}}
{{- if not (has $store (list "json" "temporal" "plugin")) -}}
{{- fail "store must be json or temporal" -}}
{{- end -}}
{{- $store -}}
{{- end -}}

{{- define "relay.claimName" -}}
{{- default (printf "%s-data" (include "relay.fullname" .)) .Values.persistence.existingClaim -}}
{{- end -}}


{{/* True when the pod has the data volume: the JSON store, a plugin command in the image (it keeps its files in /data), or a sidecar with plugin.sidecar.dataPath. */}}
{{- define "relay.dataVolume" -}}
{{- $store := include "relay.store" . -}}
{{- $p := fromYaml (include "relay.plugin" .) -}}
{{- if or (eq $store "json") (and (eq $store "plugin") (or $p.command (and $p.sidecar.enabled $p.sidecar.dataPath))) -}}true{{- end -}}
{{- end -}}

{{/* A value as text. Helm reads a number in values as a float, so a whole number prints with no exponent: 10485760, not 1.048576e+07. */}}
{{- define "relay.text" -}}
{{- if and (kindIs "float64" .) (eq . (float64 (int64 .))) -}}
{{- int64 . -}}
{{- else -}}
{{- toString . -}}
{{- end -}}
{{- end -}}
