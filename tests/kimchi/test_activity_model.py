import pytest
from src.kimchi import activity_model


@pytest.mark.parametrize("se_action", ["unknown", "session_started"])
@pytest.mark.parametrize("days_since_last_session", [None, 0, 0.79, 0.80, 1.0, 1.19, 1.2, 1.34, 1.30, 1.49, 1.50, 10.0])
@pytest.mark.parametrize("n_sessions_30d", [None, 30])
def test_update_last_session(se_action, days_since_last_session, n_sessions_30d):
    x0 = 0.0
    x1 = activity_model.update_last_session(x0, se_action, days_since_last_session, n_sessions_30d)