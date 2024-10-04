import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
import pyspark.pandas as ps
from src.common import log_utils
from src.kimchi import config

logger = log_utils.get_logger()

def get_scores(obs_data: pd.DataFrame) -> pd.DataFrame:
    scores = obs_data.groupby(["cluid", "observation_date"]).apply(pd_activity_score).reset_index()
    return scores


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


def update_score(x0: float, days_since_last_event: float | None, days_since_last_session: float | None, event_type: str, se_action: str):
    x = update_last_event(x0, days_since_last_event)
    x = update_last_session(x, days_since_last_session)
    x = update_signal(x, se_action)
    x = capping(x)
    logger.debug(f"update_score({x0:.1f}, {days_since_last_event}, {days_since_last_session}, {event_type}, {se_action}) -> {x:.1f}")
    return x


def update_last_event(x0: float, days_since_last_event: float | None) -> float:
    x = x0
    if days_since_last_event is not None:
        x -= config.decay_per_day * days_since_last_event
    return x


def update_last_session(x0: float, days_since_last_session: float | None) -> float:
    x = x0
    if days_since_last_session is not None:
        logger.debug(f"{days_since_last_session=}")
    return x


def update_signal(x0: float, se_action: str) -> float:
    x = x0 + config.signals.get(se_action, 0.0)
    return x


def capping(x0: float) -> float:
    x = x0
    x = config.score_lb if x < config.score_lb else x
    x = config.score_ub if x > config.score_ub else x
    return x
