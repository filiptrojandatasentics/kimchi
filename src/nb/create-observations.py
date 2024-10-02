# Databricks notebook source
# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

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
