import random
from collections import Counter

import pytest

from fakes import FakeStore, encoded
from mdp import layout
from mdp.hdfs import HdfsError
from mdp.stream import checkpoint, codec
from mdp.stream.sink import PartitionSink


class Crash(Exception):
    pass


def crash_at(*points, after=0):
    """Hook that raises on the (after+1)th time one of `points` is reached."""
    seen = {"n": 0}

    def hook(point):
        if point in points:
            if seen["n"] == after:
                seen["n"] += 1
                raise Crash(point)
            seen["n"] += 1

    return hook


def trusted_ids(store, partition):
    cp = checkpoint.load(store, partition)
    ids = []
    for path in checkpoint.trusted_batch_paths(cp, partition):
        ids += codec.table_from_bytes(store.read(path), ["id"]).column("id").to_pylist()
    return ids


def test_batches_get_sequential_names_and_the_checkpoint_follows():
    store = FakeStore()
    sink = PartitionSink(store, 2)
    assert sink.ingest(encoded(range(0, 5))) == 5
    assert sink.ingest(encoded(range(5, 9))) == 4
    assert sorted(store.files) == [
        layout.batch_path(2, 0), layout.batch_path(2, 1), layout.checkpoint_path(2)]
    assert checkpoint.load(store, 2) == checkpoint.Checkpoint(batch=2, offset=9)
    assert trusted_ids(store, 2) == list(range(9))


def test_fresh_sink_starts_at_offset_zero():
    assert PartitionSink(FakeStore(), 0).start_offset == 0


def test_restart_resumes_from_the_checkpoint():
    store = FakeStore()
    PartitionSink(store, 1).ingest(encoded(range(0, 10)))
    again = PartitionSink(store, 1)
    assert again.start_offset == 10 and again.cp.batch == 1


def test_crash_between_parquet_and_checkpoint_does_not_duplicate():
    store = FakeStore()
    sink = PartitionSink(store, 0, crash_at("after_parquet", after=1))
    sink.ingest(encoded(range(0, 5)))
    with pytest.raises(Crash):
        sink.ingest(encoded(range(5, 12)))

    # batch 1 is on disk but the checkpoint still says batch 1 / offset 5
    assert layout.batch_path(0, 1) in store.files
    assert checkpoint.load(store, 0) == checkpoint.Checkpoint(batch=1, offset=5)
    assert trusted_ids(store, 0) == list(range(5))

    # after restart the batch boundaries differ; the leftover file is overwritten
    restarted = PartitionSink(store, 0)
    restarted.ingest(encoded(range(5, 8)))
    restarted.ingest(encoded(range(8, 20)))
    ids = trusted_ids(store, 0)
    assert ids == list(range(20))


def test_files_past_the_checkpoint_are_not_trusted():
    store = FakeStore()
    sink = PartitionSink(store, 0, crash_at("after_parquet"))
    with pytest.raises(Crash):
        sink.ingest(encoded(range(0, 6)))
    assert layout.batch_path(0, 0) in store.files
    assert trusted_ids(store, 0) == []


def test_crash_after_checkpoint_loses_nothing():
    store = FakeStore()
    sink = PartitionSink(store, 0, crash_at("after_checkpoint"))
    with pytest.raises(Crash):
        sink.ingest(encoded(range(0, 6)))
    assert trusted_ids(store, 0) == list(range(6))
    assert PartitionSink(store, 0).start_offset == 6


def test_failed_checkpoint_rename_keeps_the_previous_checkpoint():
    store = FakeStore()
    sink = PartitionSink(store, 0)
    sink.ingest(encoded(range(0, 4)))
    store.fail_rename = True
    with pytest.raises(HdfsError):
        sink.ingest(encoded(range(4, 8)))
    store.fail_rename = False
    assert checkpoint.load(store, 0) == checkpoint.Checkpoint(batch=1, offset=4)
    assert sink.cp == checkpoint.Checkpoint(batch=1, offset=4)  # memory matches what is persisted
    sink.ingest(encoded(range(4, 8)))
    assert trusted_ids(store, 0) == list(range(8))


def test_replayed_records_below_the_checkpoint_are_ignored():
    store = FakeStore()
    sink = PartitionSink(store, 0)
    sink.ingest(encoded(range(0, 6)))
    assert sink.ingest(encoded(range(3, 6))) == 0
    assert trusted_ids(store, 0) == list(range(6))


def test_damaged_checkpoint_is_an_error_not_a_silent_restart():
    store = FakeStore()
    store.write(layout.checkpoint_path(0), b"{not json")
    with pytest.raises(ValueError):
        PartitionSink(store, 0)


@pytest.mark.parametrize("seed", range(30))
def test_random_crashes_never_duplicate_or_lose_rows(seed):
    rng = random.Random(seed)
    total = 400
    log = encoded(range(total))  # the partition: offset == id here
    store = FakeStore()
    points = ["after_parquet", "after_checkpoint"]

    def maybe_crash(point):
        if point in points and rng.random() < 0.3:
            raise Crash(point)

    restarts = 0
    while True:
        sink = PartitionSink(store, 3, maybe_crash)
        pos = sink.start_offset
        try:
            while pos < total:
                n = rng.randint(1, 40)
                batch = log[pos:pos + n]
                sink.ingest(batch)
                pos = sink.start_offset
            break
        except Crash:
            restarts += 1
    counts = Counter(trusted_ids(store, 3))
    assert sorted(counts) == list(range(total))
    assert max(counts.values()) == 1
    assert restarts > 0
