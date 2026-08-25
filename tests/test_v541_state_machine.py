import tools.run_forensic_15m_paper as base
import tools.run_forensic_15m_v54_paper as v54
from tools.run_forensic_15m_v541_paper import V541Engine


def _bare_engine(state="PASSIVE"):
    eng = object.__new__(V541Engine)
    eng._v54_state = state
    eng._v541_batch_onset_consumed = False
    eng.emit = lambda *args, **kwargs: None
    return eng


def test_same_batch_onset_is_evaluated_only_once(monkeypatch):
    eng = _bare_engine("PASSIVE")
    routed = []
    posted = []

    def fake_v54_route(self, side, px, now, up_book, down_book):
        routed.append((side, px))
        self._onset_p({})

    monkeypatch.setattr(v54.V54Engine, "_route_or_post", fake_v54_route)
    monkeypatch.setattr(v54.V54Engine, "_onset_p", lambda self, snap: 0.2)
    monkeypatch.setattr(
        base.Engine,
        "post",
        lambda self, side, px, now, **kwargs: posted.append((side, px)),
    )

    eng._route_or_post("UP", 0.49, 18.0, object(), object())
    eng._route_or_post("UP", 0.48, 18.0, object(), object())
    eng._route_or_post("UP", 0.47, 18.0, object(), object())

    assert routed == [("UP", 0.49)]
    assert posted == [("UP", 0.48), ("UP", 0.47)]
    assert eng._v541_batch_onset_consumed is True


def test_continuation_stop_blocks_onset_rearm(monkeypatch):
    eng = _bare_engine("AGGRESSIVE")
    events = []
    eng.emit = lambda now, event, **kwargs: events.append(event)

    monkeypatch.setattr(
        v54.V54Engine,
        "_end_episode",
        lambda self, now, reason: setattr(self, "_v54_state", "PASSIVE"),
    )

    eng._end_episode(20.0, "continuation hazard stopped p=0.85 u=0.99")

    assert eng._v54_state == "BLOCKED"
    assert events == ["AGGRESSIVE_ONSET_BLOCK"]


def test_passive_execution_episode_stop_does_not_block(monkeypatch):
    eng = _bare_engine("AGGRESSIVE")

    monkeypatch.setattr(
        v54.V54Engine,
        "_end_episode",
        lambda self, now, reason: setattr(self, "_v54_state", "PASSIVE"),
    )

    eng._end_episode(20.0, "passive execution intervened")

    assert eng._v54_state == "PASSIVE"


def test_real_maker_fill_rearms_blocked_onset(monkeypatch):
    eng = _bare_engine("BLOCKED")
    events = []
    eng.emit = lambda now, event, **kwargs: events.append(event)

    monkeypatch.setattr(v54.V54Engine, "fill", lambda *args, **kwargs: True)

    ok = eng.fill(
        25.0,
        "DOWN",
        0.5,
        0.52,
        "MAKER_FILL",
        "public tape execution",
    )

    assert ok is True
    assert eng._v54_state == "PASSIVE"
    assert events == ["AGGRESSIVE_ONSET_UNBLOCK"]
