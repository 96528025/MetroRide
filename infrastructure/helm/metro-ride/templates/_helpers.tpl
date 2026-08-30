{{/*
Fully-qualified image reference for a core service.

Renders <registry>/<namespace>/<namePrefix><service>:<tag>, omitting the
registry or namespace when they are empty. The tag falls back to the chart
appVersion so a rendered manifest is never tagless, but CI always passes the
full commit SHA explicitly.

Usage: {{ include "metroride.image" (dict "root" $ "service" $name) }}
*/}}
{{- define "metroride.image" -}}
{{- $root := .root -}}
{{- $image := $root.Values.image -}}
{{- $tag := $image.tag | default $root.Chart.AppVersion -}}
{{- $path := printf "%s%s" $image.namePrefix .service -}}
{{- with $image.namespace -}}
{{- $path = printf "%s/%s" . $path -}}
{{- end -}}
{{- with $image.registry -}}
{{- $path = printf "%s/%s" . $path -}}
{{- end -}}
{{- printf "%s:%s" $path $tag -}}
{{- end -}}

{{/*
Common labels applied to every object the chart owns.
*/}}
{{- define "metroride.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/*
Init containers that block a pod from starting until the data stores it needs
are actually serving. Each check speaks the dependency's own protocol
(redis-cli PING, pg_isready) using an image the cluster already has, so no
extra image and no `nc -z` portability assumption are introduced.

Bounded by dependencyWait.timeoutSeconds: the container fails loudly on
timeout instead of sleeping for a fixed period.

Usage: {{ include "metroride.dependencyWait" (dict "root" $ "dependsOn" $svc.dependsOn) }}
*/}}
{{- define "metroride.dependencyWait" -}}
{{- $root := .root -}}
{{- $wait := $root.Values.dependencyWait -}}
{{- if and $wait.enabled .dependsOn -}}
initContainers:
{{- range .dependsOn }}
{{- if eq . "redis" }}
  - name: wait-for-redis
    image: {{ $wait.redisImage | quote }}
    imagePullPolicy: {{ $root.Values.image.pullPolicy }}
    command: ["/bin/sh", "-c"]
    args:
      - |
        deadline=$(( $(date +%s) + {{ $wait.timeoutSeconds | int }} ))
        until redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping | grep -q PONG; do
          if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "failed: redis at $REDIS_HOST:$REDIS_PORT not ready within {{ $wait.timeoutSeconds }}s" >&2
            exit 1
          fi
          echo "waiting for redis at $REDIS_HOST:$REDIS_PORT..."
          sleep 2
        done
        echo "ok: redis is serving"
    env:
      - name: REDIS_HOST
        value: {{ (splitList ":" $root.Values.config.redisAddr) | first | quote }}
      - name: REDIS_PORT
        value: {{ (splitList ":" $root.Values.config.redisAddr) | last | quote }}
    resources:
      {{- toYaml $wait.resources | nindent 6 }}
{{- end }}
{{- if eq . "postgres" }}
  - name: wait-for-postgres
    image: {{ $wait.postgresImage | quote }}
    imagePullPolicy: {{ $root.Values.image.pullPolicy }}
    command: ["/bin/sh", "-c"]
    args:
      - |
        deadline=$(( $(date +%s) + {{ $wait.timeoutSeconds | int }} ))
        until pg_isready -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE"; do
          if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "failed: postgres at $PGHOST not ready within {{ $wait.timeoutSeconds }}s" >&2
            exit 1
          fi
          echo "waiting for postgres at $PGHOST..."
          sleep 2
        done
        echo "ok: postgres is accepting connections"
    env:
      - name: PGHOST
        value: {{ $wait.postgresHost | quote }}
      - name: PGUSER
        value: {{ $root.Values.dependencies.postgres.username | quote }}
      - name: PGDATABASE
        value: {{ $root.Values.dependencies.postgres.database | quote }}
    resources:
      {{- toYaml $wait.resources | nindent 6 }}
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}
