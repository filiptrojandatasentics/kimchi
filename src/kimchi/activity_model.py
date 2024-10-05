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
    score = 0.0
    for x in df.itertuples():
        score = update_score(score, x)
    res = pd.Series(dict(score=score))
    logger.debug(f"final score: {score:.1f}")
    return res


def update_score(x0: float, f: tuple) -> float:
    x = update_last_event(x0, f.days_since_last_event)
    x = update_last_session(x, f.se_action, f.days_since_last_session, f.n_sessions_30d)
    x = update_signal(x, f.se_action)
    x = capping(x)
    logger.debug(f"update_score({x0:.1f}, {f}) -> {x:.1f}")
    return x


def update_last_event(x0: float, days_since_last_event: float | None) -> float:
    x = x0
    if days_since_last_event is not None:
        x -= config.decay_per_day * days_since_last_event
    return x


def update_last_session(x0: float, se_action: str, days_since_last_session: float | None, n_sessions_30d: float | None) -> float:
    x = x0
    if se_action == "session_started" and days_since_last_session and n_sessions_30d:
        avg_days_between_sessions_30d = 30 / (n_sessions_30d + 1)
        last_session_delay = days_since_last_session / avg_days_between_sessions_30d - 1
        for r in config.session_delay_rule:
            (lb, ub, pts) = r
            if last_session_delay >= lb and last_session_delay < ub:
                x += pts
                logger.debug(f"{last_session_delay=}: {pts} points added")
    return x


def update_signal(x0: float, se_action: str) -> float:
    x = x0 + config.signals.get(se_action, 0.0)
    return x


def capping(x0: float) -> float:
    x = x0
    x = config.score_lb if x < config.score_lb else x
    x = config.score_ub if x > config.score_ub else x
    return x
