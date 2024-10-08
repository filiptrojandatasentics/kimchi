import pandas as pd
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
    d1 = delta_last_event(f.days_since_last_event)
    d2 = delta_last_session(f.se_action, f.days_since_last_session, f.n_sessions_30d)
    d3 = update_signal(f.se_action)
    d = d1 + d2 + d3
    x = x0 + d
    x = capping(x)
    logger.debug(f"update_score({f}): {x0:.1f} {d:+.1f} = {x:.1f}")
    return x


def delta_last_event(days_since_last_event: float | None) -> float:
    if days_since_last_event is not None:
        d = -config.decay_per_day * days_since_last_event
    else:
        d = 0.0
    return d


def delta_last_session(se_action: str, days_since_last_session: float | None, n_sessions_30d: float | None) -> float:
    d = 0.0
    if se_action == "session_started" and days_since_last_session is not None and n_sessions_30d is not None:
        avg_days_between_sessions_30d = 30 / (n_sessions_30d + 1)
        last_session_delay = days_since_last_session / avg_days_between_sessions_30d - 1
        logger.info(f"{last_session_delay=}")
        for r in config.session_delay_rule:
            (lb, ub, pts) = r
            if last_session_delay >= lb and last_session_delay < ub:
                d = pts
                logger.info(f"{last_session_delay=}: {pts} points added")
    return d


def update_signal(se_action: str) -> float:
    d = config.signals.get(se_action, 0.0)
    return d


def capping(x0: float) -> float:
    x = x0
    x = config.score_lb if x < config.score_lb else x
    x = config.score_ub if x > config.score_ub else x
    return x
