# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table kimchi.silver.obs_data as
# MAGIC with a as (
# MAGIC   select * 
# MAGIC   from kimchi.silver.obs 
# MAGIC   --limit 1000  -- use limit X to limit the number of input observations
# MAGIC )
# MAGIC select a.cluid,
# MAGIC   a.observation_date,
# MAGIC   b.tst,
# MAGIC   b.event_type,
# MAGIC   b.se_action,
# MAGIC   b.n_sessions_30d,
# MAGIC   b.days_since_last_event,
# MAGIC   b.days_since_last_session
# MAGIC from a
# MAGIC inner join kimchi.silver.lux_features b 
# MAGIC on a.cluid = b.cluid and b.tst between date_add(a.observation_date, -90) and a.observation_date;
# MAGIC
# MAGIC select count(1) from kimchi.silver.obs_data;

# COMMAND ----------

# MAGIC %md
# MAGIC https://stackoverflow.com/questions/40006395/applying-udfs-on-groupeddata-in-pyspark-with-functioning-python-example

# COMMAND ----------

import os
os.chdir("/Workspace/Users/filip.trojan@datasentics.com/kimchi")

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pyspark.pandas as ps
from src.common import log_utils
from src.kimchi import activity_model
logger = log_utils.get_script_logger()

# COMMAND ----------

logger.info("started")
obs_data = spark.sql("""
    select cluid, 
        observation_date, 
        tst, 
        event_type, 
        se_action,
        n_sessions_30d,
        days_since_last_event,
        days_since_last_session
    from kimchi.silver.obs_data 
    order by cluid, observation_date, tst
    """
).toPandas()
scores = activity_model.get_scores(obs_data)

# COMMAND ----------

ps.DataFrame(scores).to_table("kimchi.gold.activity_score")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kimchi.gold.activity_score order by cluid, observation_date;
