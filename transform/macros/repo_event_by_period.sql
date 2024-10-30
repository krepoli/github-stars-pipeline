{% macro repo_event_by_period(event_type, period) -%} 
  Select distinct 
    repo_id,
    repo_name,
    date_trunc('{{ period }}', event_date) as date_truncation,
    count(*) as event_count,
    sum(event_count) over (partition by repo_id order by date_truncation) as cumul_count
    from {{ ref('stg_gharchive') }}
    where event_type = '{{ event_type }}'
    group by 1, 2, 3
{%- endmacro %}