create or replace table kimchi.silver.obs_data as
with a as (
  select * from kimchi.silver.obs
  -- limit 1000
)
select a.cluid,
  a.observation_date,
  b.tst,
  b.event_type,
  b.se_action,
  b.n_sessions_30d,
  b.days_since_last_event,
  b.days_since_last_session
from a
inner join kimchi.silver.lux_features b 
on a.cluid = b.cluid and b.tst between date_add(a.observation_date, -90) and a.observation_date;