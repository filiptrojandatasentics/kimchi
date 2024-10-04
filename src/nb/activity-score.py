# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table kimchi.default.obs_data as
# MAGIC select a.cluid,
# MAGIC   a.observation_date,
# MAGIC   b.tst,
# MAGIC   b.event_type,
# MAGIC   b.se_action
# MAGIC from (select * from kimchi.default.obs) a  -- use limit X to limit the number of observations
# MAGIC inner join kimchi.default.lux b 
# MAGIC on a.cluid = b.cluid and b.tst between date_add(a.observation_date, -90) and a.observation_date;
# MAGIC
# MAGIC select count(1) from kimchi.default.obs_data;

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
logger = log_utils.get_script_logger()

# COMMAND ----------

main_signals = {
    "session_started": 20,
    "securities_account_overview_displayed": 15,
    "portfolio_overview_displayed": 15,
    "product_selection_confirmed": 7.5,
    "securities_orders_button_selected": 7.5,
    "watchlist_menu_button_selected": 7.5,
    "individual_recategorization_displayed": 5,
    "bulk_recategorization_triggered": 5,
    "individual_recategorization_triggered": 5,
    "feedback_selected": 5,
    "inbox_opened": 3,
}

minor_signals = [
    "dashboard_item_visibility_changed",
    "my_spendings_detail_displayed",
    "spotlight_detail_displayed",
    "insight_presented_on_overview",
    "instant_cash_offer_selected",
    "spending_budget_screen_displayed",
    "bulk_recategorization_displayed",
    "interval_changed",
    "insight_open"
    "local_use_case_selected",
    "product_selection_displayed"
    "spendings_tab_switched",
    "transaction_screen_interval_changed",
    "merchant_transactions_screen_displayed",
    "bulk_recategorization_triggered",
    "info_button_selected",
    "statements_button_selected",
    "store_landing_page_displayed",
]

def update_score(s0: float, days_since_last_event: float | None, days_since_last_session: float | None, event_type: str, se_action: str):
    x = s0
    if days_since_last_event is not None:
        x -= 5.0 * days_since_last_event
    if days_since_last_session is not None:
        logger.debug(f"{days_since_last_session=}")
    for k, v in main_signals.items():
        if se_action == k:
            x += 1.0 * v
            logger.debug(f"main signal {k} {v:+1.1f}")
    for s in minor_signals:
        if se_action == s:
            x += 2.0
            logger.debug(f"minor signal {s}")
    # capping
    x = -50 if x < -50 else x
    x = +50 if x > +50 else x
    logger.debug(f"update_score({s0:.1f}, {days_since_last_event}, {days_since_last_session}, {event_type}, {se_action}) -> {x:.1f}")
    return x

def pd_activity_score(df: pd.DataFrame) -> pd.Series:
    days_history = (df.tst.max() - df.tst.min()).days
    score = 0.0
    tst_last_session: pd.Timestamp | None = None
    tst_last_event: pd.Timestamp | None = None
    days_since_last_session: float | None = None
    days_since_last_event: float | None = None
    for x in df.itertuples():
        score = update_score(score, days_since_last_event, days_since_last_session, x.event_type, x.se_action)
        if x.se_action == "session_started":
            if tst_last_session is not None:
                days_since_last_session = (x.tst - tst_last_session).total_seconds() / 86400
            tst_last_session = x.tst
        if tst_last_event is not None:
            days_since_last_event = (x.tst - tst_last_event).total_seconds() / 86400
        tst_last_event = x.tst
    res = pd.Series(dict(score=score))
    logger.debug(f"final score: {score:.1f}")
    return res

# COMMAND ----------

logger.info("started")
obs_data = spark.sql("""
    select cluid, observation_date, tst, event_type, se_action 
    from kimchi.default.obs_data 
    order by cluid, observation_date, tst
    """
).toPandas()
scores = obs_data.groupby(["cluid", "observation_date"]).apply(pd_activity_score).reset_index()

# COMMAND ----------

ps.DataFrame(scores).to_table("kimchi.default.activity_score")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kimchi.default.activity_score;
