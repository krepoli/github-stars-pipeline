Select distinct
  repo_id,
  repo_name,
  date_day,
  star_count,
  sum(star_count) over (partition by repo_id order by date_day) AS cumul_star_count
from (
  Select 
    repo_id, 
    repo_name, 
    date_trunc('day', event_date) as date_day,
    count(*) as star_count 
  from {{ ref('stg_gharchive') }} 
  where event_type = 'Watch'
  group by repo_id, repo_name, date_day
    )