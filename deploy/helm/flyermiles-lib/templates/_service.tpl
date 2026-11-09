{{/*
Everything a FlyerMiles service needs. A service chart's only template is:
  {{ include "flyermiles-lib.all" . }}
*/}}
{{- define "flyermiles-lib.all" -}}
{{ include "flyermiles-lib.configmap" . }}
{{- if .Values.database }}
---
{{ include "flyermiles-lib.secret" . }}
{{- end }}
---
{{ include "flyermiles-lib.deployment" . }}
---
{{ include "flyermiles-lib.service" . }}
{{- if .Values.pdb.enabled }}
---
{{ include "flyermiles-lib.pdb" . }}
{{- end }}
{{- if .Values.serviceMonitor.enabled }}
---
{{ include "flyermiles-lib.servicemonitor" . }}
{{- end }}
{{- end -}}

{{- define "flyermiles-lib.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
data:
  {{- range $k, $v := .Values.config }}
  {{ $k }}: {{ $v | quote }}
  {{- end }}
{{- end -}}

{{- define "flyermiles-lib.secret" -}}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
type: Opaque
stringData:
  DB_USER: {{ .Values.database.user | quote }}
  DB_PASSWORD: {{ .Values.database.password | quote }}
{{- end -}}

{{- define "flyermiles-lib.deployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
spec:
  {{- if not .Values.autoscaling.enabled }}
  replicas: {{ .Values.replicas }}
  {{- end }}
  revisionHistoryLimit: 3
  selector:
    matchLabels:
      {{- include "flyermiles-lib.selectorLabels" . | nindent 6 }}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      # Zero-downtime: a new pod must be Ready before an old one is taken away.
      maxUnavailable: 0
      maxSurge: 1
  template:
    metadata:
      labels:
        {{- include "flyermiles-lib.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/part-of: flyermiles
      annotations:
        checksum/config: {{ toJson .Values.config | sha256sum }}
    spec:
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 1001
        fsGroup: 1001
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              {{- include "flyermiles-lib.selectorLabels" . | nindent 14 }}
      containers:
        - name: app
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - name: http
              containerPort: {{ .Values.port }}
          envFrom:
            - configMapRef:
                name: {{ include "flyermiles-lib.name" . }}
          env:
            {{- include "flyermiles-lib.podEnv" . | nindent 12 }}
            {{- include "flyermiles-lib.env" . | nindent 12 }}
          # Actuator health groups: liveness = the JVM is alive; readiness = its dependencies are.
          startupProbe:
            httpGet: { path: /actuator/health/liveness, port: http }
            periodSeconds: 3
            failureThreshold: 60
          livenessProbe:
            httpGet: { path: /actuator/health/liveness, port: http }
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: /actuator/health/readiness, port: http }
            periodSeconds: 5
            failureThreshold: 2
          lifecycle:
            preStop:
              # Let the endpoint removal propagate before Spring starts its graceful shutdown.
              exec: { command: ["sleep", "5"] }
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          {{- with .Values.volumeMounts }}
          volumeMounts:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      {{- with .Values.volumes }}
      volumes:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end -}}

{{- define "flyermiles-lib.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
spec:
  selector:
    {{- include "flyermiles-lib.selectorLabels" . | nindent 4 }}
  ports:
    - name: http
      port: {{ .Values.port }}
      targetPort: http
{{- end -}}

{{- define "flyermiles-lib.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
spec:
  {{- if .Values.pdb.minAvailable }}
  minAvailable: {{ .Values.pdb.minAvailable }}
  {{- else }}
  maxUnavailable: {{ .Values.pdb.maxUnavailable | default 1 }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "flyermiles-lib.selectorLabels" . | nindent 6 }}
{{- end -}}

{{- define "flyermiles-lib.servicemonitor" -}}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "flyermiles-lib.name" . }}
  labels:
    {{- include "flyermiles-lib.labels" . | nindent 4 }}
spec:
  selector:
    matchLabels:
      {{- include "flyermiles-lib.selectorLabels" . | nindent 6 }}
  endpoints:
    - port: http
      path: /actuator/prometheus
      interval: 10s
{{- end -}}
