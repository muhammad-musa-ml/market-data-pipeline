import sys
import types

import cassandra

try:
    import cassandra.cluster  # noqa: F401
except cassandra.DependencyException:
    # Windows wheels ship no libev, and Python 3.12 dropped asyncore, so the driver finds no
    # default connection class and refuses to import. The unit tests never open a connection;
    # a stand-in module is enough to get the package imported. Linux (CI, the containers) has libev.
    stub = types.ModuleType("asyncore")
    stub.dispatcher = type("dispatcher", (), {})
    sys.modules["asyncore"] = stub
    import cassandra.cluster  # noqa: F401
