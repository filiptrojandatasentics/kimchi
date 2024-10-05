# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table kimchi.silver.lux_features as
# MAGIC with session as (
# MAGIC   select cluid,
# MAGIC     tst
# MAGIC   from kimchi.default.lux
# MAGIC   where se_action = 'session_started'
# MAGIC ), session_lag as (
# MAGIC   select cluid,
# MAGIC     tst as tst_session,
# MAGIC     lag(tst) over (partition by cluid order by tst) as tst_last_session
# MAGIC   from session
# MAGIC ), w as (
# MAGIC   select a.cluid,
# MAGIC     a.tst,
# MAGIC     count(b.tst) as n_sessions_30d
# MAGIC   from kimchi.default.lux a
# MAGIC   left join session b on a.cluid = b.cluid and datediff(second, b.tst, a.tst) between 0 and 30*86400
# MAGIC   group by a.cluid, a.tst
# MAGIC )
# MAGIC select e.cluid,
# MAGIC   e.tst,
# MAGIC   e.event_type,
# MAGIC   e.se_action,
# MAGIC   w.n_sessions_30d,
# MAGIC   datediff(second, lag(e.tst) over (partition by e.cluid order by e.tst), e.tst)/86400 as days_since_last_event,
# MAGIC   datediff(second, s.tst_last_session, e.tst)/86400 as days_since_last_session
# MAGIC from kimchi.default.lux e
# MAGIC left join w on e.cluid = w.cluid and e.tst = w.tst
# MAGIC left join session_lag s on e.cluid = s.cluid and e.tst >= s.tst_last_session and e.tst < s.tst_session
# MAGIC order by e.cluid, e.tst

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kimchi.silver.lux_features;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kimchi.silver.lux_features order by cluid, tst;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kimchi.silver.obs as
# MAGIC select e.cluid, d.observation_date
# MAGIC from (select distinct cluid from kimchi.default.lux) e
# MAGIC cross join (
# MAGIC   select date_valid as observation_date from kimchi.default.calendar where dow = 2  -- Monday
# MAGIC ) d;
# MAGIC
# MAGIC select count(1) from kimchi.silver.obs;
