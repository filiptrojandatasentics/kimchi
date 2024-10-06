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
logger.info("loading obs_data")
obs_data = get_obs_data()
logger.info(f"calculating score with {len(obs_data):,} input events")
scores = activity_model.get_scores(obs_data)
write_scores(scores)
logger.info("finished")
