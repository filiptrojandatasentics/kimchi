# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE kimchi.default.lux (
# MAGIC     cluid varchar(32),
# MAGIC     tst timestamp,
# MAGIC     event_type varchar(64),
# MAGIC     se_action varchar(64)
# MAGIC );

# COMMAND ----------

lkp_event_type = [
    "call_application",
    "close_application",
    "page_view",
    "customer_contact",
    "digital_action",
    "marketing_contact",
    "store-card-clicked",
    "self_service",
    "customer_needs",
    "copypaste",
    "store-landingpage-viewed",
    "modifyvaluethemiddle",
    "enterkeypress",
    "ecommerce_action",
    "pv",
    "transaction_completion",
]

# COMMAND ----------

len(lkp_event_type)

# COMMAND ----------

lkp_se_action = [
    "session_started",
    "logout",
    "insight_presented_on_overview",
    "authentication",
    "portfolio_overview_displayed",
    "promotion_seen",
    "store-item-clicked",
    "store-card-clicked",
    "spotlight_detail_displayed",
    "dashboard_demo_displayed",
    "my_spendings_detail_displayed",
    "credit_opened",
    "promotion_clicked",
    "transactions_screen_displayed",
    "interval_changed",
    "instant_cash_flow_started",
    "local_store_flow_started",
    "user_mind",
    "ftt_overview_displayed",
    "local_use_case_selected",
    "flow_opened",
    "individual_recategorization_displayed",
    "promotion_selected",
    "spending_tag_switched",
    "george_dialog_opened",
    "transaction_selected",
    "security_sell_order_started",
    "individual_recategorization_started",
    "bulk_recategorization_displayed",
    "security_buy_order_started",
    "spendings_budgets_displayed",
    "instant_cash_bottom_sheet_displayed",
    "sell_usd_selected",
    "security_sell_order_signed",
    "spendings_budget_screen_displayed",
    "promotion_intermodal_action_selected",
    "export_selected",
    "security_buy_order_signed",
    "recategorization_confirmed",
    "product_selection_started",
    "transactions_screen_interval_changed",
    "tuning_started",
    "rating_started",
    "product_selection_cancelled",
    "spendings_budget_changed",
    "spendings_budget_settings_selected",
    "analyze_screen_selected",
    "card_added",
    "securities_orders_button_selected",
    "edit_spendings_budget_selected",
    "spendings_budget_creation_dropped",
    "rule_recategorization_confirmed",
    "securities_account_overview_displayed",
    "analyze_shortcut_selected",
    "my_income_source_selected",
    "edit_external_saving_selected",
    "store_landing_page_displayed",
    "statement_download",
    "settings_selected",
    "edit_external_investment_selected",
    "merchant_transactions_screen_displayed",
    "statement_button_selected",
    "sell_restart_order_selected",
    "exports_investments_selected",
    "products_selected",
    "info_button_selected",
    "my_income_interval_changed",
    "watch_list_header_selected",
    "security_order_cancelled",
    "product_selection_confirmed",
    "checkout_started",
    "agent_connected",
    "store_grouping_page_displayed",
    "start_selected",
    "open_store_selected",
    "overview_appearance_changed",
    "start_contacted",
    "product_selection_selected",
    "customer_rejected",
    "security_details_displayed",
    "emobility_auto_sales_started",
    "customer_feedback",
    "price_alert_menu_button_selected",
    "onboarded",
    "checkout_opened",
    "set_up_round_up_selected",
    "view",
    "human_chat_deactivated",
    "completed",
    "opt_out_selected",
    "set_up_savings_account_selected",
    "store_grouping_button_selected",
    "price_detail_page_displayed",
    "investment_offer_selected",
    "application_started",
    "why_invest_selected",
    "contact_bank_selected",
    "poc_started",
    "private_banking_button_selected",
    "card_management_started",
    "emobility_auto_otp_contacted",
    "contactform_opened",
    "customer_data_entered",
    "poc_card_selected",
    "card_replacement_started",
    "precontract_email_sent",
    "finance_plan_started",
    "product_signed",
    "customer_checked",
    "selected",
    "electronic_delivery_activated",
    "emobility_moto_otp_sales_started",
    "contactform_sent",
    "customer_data",
    "customer_summary",
    "emobility_auto_ptk_sales_ended",
    "product_offer",
    "emobility_auto_sales_ended",
    "emobility_moto_otp_sales_ended",
    "emobility_auto_otp_sales_ended",
    "leasinglocations_opened",
    "sales_started",
]

# COMMAND ----------

len(lkp_se_action)

# COMMAND ----------

import numpy as np
import pandas as pd
import scipy.special

# COMMAND ----------

p_se_action = scipy.special.softmax(np.exp(np.arange(123, 1, -1) / 60))
p_se_action

# COMMAND ----------

p_se_action[0] / p_se_action[-1]

# COMMAND ----------

sum(p_se_action)

# COMMAND ----------

action_df = pd.DataFrame({
    "se_action": lkp_se_action,
    "prob": p_se_action,
})
action_df.plot.bar(x="se_action", y="prob")

# COMMAND ----------

p_event_type = scipy.special.softmax(np.exp(np.arange(17, 1, -1) / 10))
p_event_type

# COMMAND ----------

p_event_type[0] / p_event_type[-1]

# COMMAND ----------

sum(p_event_type)

# COMMAND ----------

event_type_df = pd.DataFrame({
    "event_type": lkp_event_type,
    "prob": p_event_type,
})
event_type_df.plot.bar(x="event_type", y="prob")

# COMMAND ----------

rng = np.random.default_rng(42)

# COMMAND ----------

n_cluid = 1000
events_per_day = rng.lognormal(mean=1.0, sigma=1.0, size=n_cluid)
n_events_df = pd.DataFrame({"cluid": range(n_cluid), "events_per_day": events_per_day}).sort_values(by="events_per_day")
n_events_df.describe()

# COMMAND ----------

date_start = pd.Timestamp(2024, 1, 1)
date_end = pd.Timestamp(2024, 12, 31)

def ts(x: float) -> pd.Timestamp:
    y = date_start + x * (date_end - date_start)
    return y

ts(0.569).isoformat()

# COMMAND ----------

n_days = (date_end - date_start).days
n_days

# COMMAND ----------

res_list = []
for x in n_events_df.itertuples():
    cluid = f"{x.cluid:08d}"
    avg_events_per_day = x.events_per_day
    events_per_day = np.random.poisson(avg_events_per_day, n_days)
    n_events = sum(events_per_day)
    t = [ts(x) for x in np.sort(rng.uniform(0, 1, n_events))]
    event_type = rng.choice(lkp_event_type, n_events, p=p_event_type)
    se_action = rng.choice(lkp_se_action, n_events, p=p_se_action)
    cluid_df = pd.DataFrame({
        "cluid": cluid, 
        "tst": t, 
        "event_type": event_type, 
        "se_action": se_action,
    })
    res_list.append(cluid_df)

# COMMAND ----------

len(res_list)

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

res_df = pd.concat(res_list)
res_df.describe()

# COMMAND ----------

# save as table
ps.DataFrame(res_df).to_table("kimchi.default.lux")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as n_events,
# MAGIC     count(distinct cluid) as n_cluid
# MAGIC from kimchi.default.lux;

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_type, count(1) n_events
# MAGIC from kimchi.default.lux
# MAGIC group by event_type order by n_events desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select se_action, count(1) n_events
# MAGIC from kimchi.default.lux
# MAGIC group by se_action order by n_events desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select cluid, least(count(1)/365, 20) avg_events_per_day
# MAGIC from kimchi.default.lux
# MAGIC group by cluid order by avg_events_per_day desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC with a as (
# MAGIC select cast(tst as date) date_valid, *
# MAGIC from kimchi.default.lux
# MAGIC )
# MAGIC select date_valid, dayofweek(date_valid), count(1) n_events
# MAGIC from a group by date_valid, dayofweek(date_valid) order by 1;

# COMMAND ----------

from pyspark.sql.functions import sequence, to_date, explode, col, weekofyear, dayofweek, year
df = spark.sql("SELECT sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day) as date_valid")
df = df.withColumn("date_valid", explode(col("date_valid")))
df = df.withColumn("year", year(col("date_valid")))
df = df.withColumn("week", weekofyear(col("date_valid")))
df = df.withColumn("dow", dayofweek(col("date_valid")))
df.write.format("delta").mode("overwrite").saveAsTable("kimchi.default.calendar")
