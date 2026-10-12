"""One Cassandra session per process."""
import threading
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable, Session
from cassandra.policies import ConstantReconnectionPolicy

from mdp.serving.settings import ServingSettings

_lock = threading.Lock()
_session: Session | None = None


def connect(cfg: ServingSettings, wait: float = 0.0,
            consistency: ConsistencyLevel = ConsistencyLevel.QUORUM) -> Session:
    """Open a session, retrying for up to `wait` seconds while the cluster comes up."""
    profile = ExecutionProfile(consistency_level=consistency, request_timeout=cfg.cassandra_timeout)
    deadline = time.monotonic() + wait
    while True:
        # a downed node that comes back should rejoin the pool within seconds, not minutes
        cluster = Cluster(list(cfg.cassandra_hosts), port=cfg.cassandra_port,
                          execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                          reconnection_policy=ConstantReconnectionPolicy(2.0),
                          connect_timeout=5, max_schema_agreement_wait=30)
        try:
            return cluster.connect()
        except NoHostAvailable:
            cluster.shutdown()
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


def shared(cfg: ServingSettings) -> Session:
    global _session
    with _lock:
        if _session is None or _session.is_shutdown:
            _session = connect(cfg)
        return _session
