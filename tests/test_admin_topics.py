"""ensure_topics on a cluster that has just created the topic and does not report its partitions yet."""
from types import SimpleNamespace

import pytest

from mdp.config import Settings
from mdp.stream import admin


class Done:
    def result(self):
        return None


class FakeAdmin:
    def __init__(self, partition_counts):
        self.counts = list(partition_counts)
        self.topic = "market.bars.v1"
        self.listed = 0

    def list_topics(self, topic=None, timeout=None):
        self.listed += 1
        if topic is None:  # the broker count used by wait_for_brokers
            return SimpleNamespace(brokers={1: 1, 2: 2, 3: 3}, topics={})
        n = self.counts.pop(0) if len(self.counts) > 1 else self.counts[0]
        return SimpleNamespace(brokers={}, topics={topic: SimpleNamespace(partitions={i: i for i in range(n)})})

    def create_topics(self, topics):
        return {t.topic: Done() for t in topics}


@pytest.fixture
def cfg():
    return Settings.from_env()


def patched(monkeypatch, fake, clock=None):
    monkeypatch.setattr(admin, "AdminClient", lambda conf: fake)
    monkeypatch.setattr(admin.time, "sleep", lambda s: None)
    if clock:
        monkeypatch.setattr(admin.time, "monotonic", clock)


def test_waits_until_the_partitions_are_reported(cfg, monkeypatch):
    fake = FakeAdmin([0, 0, cfg.partitions])
    patched(monkeypatch, fake)
    admin.ensure_topics(cfg)
    assert fake.listed >= 4  # one for the brokers, three looks at the topic


def test_gives_up_if_the_partition_count_never_matches(cfg, monkeypatch):
    ticks = iter(range(0, 10_000, 5))
    patched(monkeypatch, FakeAdmin([cfg.partitions - 1]), clock=lambda: next(ticks))
    with pytest.raises(RuntimeError, match="expected"):
        admin.ensure_topics(cfg)


def test_returns_at_once_when_the_topic_is_already_right(cfg, monkeypatch):
    fake = FakeAdmin([cfg.partitions])
    patched(monkeypatch, fake)
    admin.ensure_topics(cfg)
    assert fake.listed == 2
