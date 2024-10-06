import os
os.chdir("/Workspace/Users/filip.trojan@datasentics.com/kimchi")
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pyspark.pandas as ps
from src.common import log_utils
from src.kimchi import activity_model
import warnings
logger = log_utils.get_script_logger()


def sql_get_obs(limit: int | None = None) -> str:
    q_limit = f" limit {limit}" if limit else " limit 1000000"
    q = "select * from kimchi.silver.obs" + q_limit
    return q


def create_obs_data(limit: int | None = None) -> None:
    logger.info(f"creating observation data with {limit=}")
    obs_q = sql_get_obs(limit)
    data_q = f"""
    create or replace table kimchi.silver.obs_data as
    with a as ({obs_q})
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
    on a.cluid = b.cluid and b.tst between date_add(a.observation_date, -90) and a.observation_date;"""
    logger.debug(data_q)
    spark.sql(data_q)


def get_obs_data() -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
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
    return obs_data


def write_scores(scores: pd.DataFrame) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ps.DataFrame(scores).to_table("kimchi.gold.activity_score")

logger.info("started")
max_observations = None
create_obs_data(max_observations)
logger.info("loading obs_data")
obs_data = get_obs_data()
logger.info(f"calculating score with {len(obs_data):,} input events")
scores = activity_model.get_scores(obs_data)
write_scores(scores)
logger.info("finished")
