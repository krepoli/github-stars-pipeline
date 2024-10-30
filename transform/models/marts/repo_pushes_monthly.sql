-- depends_on: {{ ref('stg_gharchive') }}
{{ repo_event_by_period("push", "month") }}