# Databricks notebook source
# MAGIC %sql
# MAGIC select count(1)
# MAGIC from kimchi.default.lux
# MAGIC where se_action = 'session_started'

# COMMAND ----------

# MAGIC %sql
# MAGIC with session as (
# MAGIC   select cluid,
# MAGIC     tst
# MAGIC   from kimchi.default.lux
# MAGIC   where se_action = 'session_started'
# MAGIC ), session_lag as (
# MAGIC   select cluid,
# MAGIC     tst,
# MAGIC     lag(tst) over (partition by cluid order by tst) as tst_prev
# MAGIC   from session
# MAGIC )
# MAGIC
# MAGIC select cluid,
# MAGIC   tst,
# MAGIC   tst_prev,
# MAGIC   (bigint(tst) - bigint(tst_prev))/(3600*24) as days_between_sessions
# MAGIC from session_lag
# MAGIC order by cluid, tst

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kimchi.default.obs as
# MAGIC select e.cluid, d.observation_date
# MAGIC from (select distinct cluid from kimchi.default.lux) e
# MAGIC cross join (
# MAGIC   select date_valid as observation_date from kimchi.default.calendar where dow = 2  -- Monday
# MAGIC ) d;
# MAGIC
# MAGIC select count(1) from kimchi.default.obs;
