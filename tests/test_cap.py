import pytest
import time
import kvstore_pb2
from client import ReplicatedKVClient


@pytest.fixture
def dc1_nodes():
    return ["localhost:50051", "localhost:50052"]

@pytest.fixture
def dc2_nodes():
    return ["localhost:50053"]

@pytest.fixture
def all_nodes(dc1_nodes, dc2_nodes):
    return dc1_nodes + dc2_nodes

def test_cp_write_majority_success(dc1_nodes):
    client = ReplicatedKVClient(replica_addresses=dc1_nodes)
    ok, acks, err = client.put("key1", "val1", strategy=kvstore_pb2.QUORUM_CP)
    assert ok is True
    assert acks >= 2

def test_cp_write_minority_failure(dc2_nodes):
    client = ReplicatedKVClient(replica_addresses=dc2_nodes)
    ok, acks, err = client.put("key2", "val2", strategy=kvstore_pb2.QUORUM_CP)
    assert ok is False
    assert "Quorum loss" in err

def test_ap_write_minority_success(dc2_nodes):
    client = ReplicatedKVClient(replica_addresses=dc2_nodes)
    ok, acks, err = client.put("key3", "val3_ap", strategy=kvstore_pb2.ALWAYS_AP)
    assert ok is True
    assert acks == 1

def test_lww_conflict_resolution(all_nodes):
    client = ReplicatedKVClient(replica_addresses=all_nodes)
    client.put("key_lww", "old_val", strategy=kvstore_pb2.ALWAYS_AP)
    time.sleep(0.01)
    client.put("key_lww", "new_val", strategy=kvstore_pb2.ALWAYS_AP)
    
    found, val, err = client.get("key_lww", consistency=kvstore_pb2.FOLLOWER_STALE, target_addr="localhost:50053")
    assert found is True
    assert val == "new_val"

def test_linearizable_read_from_leaseholder(all_nodes):
    client = ReplicatedKVClient(replica_addresses=all_nodes, leaseholder_addr="localhost:50051")
    client.put("key_lin", "lin_val", strategy=kvstore_pb2.QUORUM_CP)
    
    found, val, err = client.get("key_lin", consistency=kvstore_pb2.LINEARIZABLE)
    assert found is True
    assert val == "lin_val"

def test_follower_stale_read(all_nodes):
    client = ReplicatedKVClient(replica_addresses=all_nodes)
    client.put("key_stale", "stale_val", strategy=kvstore_pb2.QUORUM_CP)
    
    found, val, err = client.get("key_stale", consistency=kvstore_pb2.FOLLOWER_STALE, target_addr="localhost:50052")
    assert found is True
    assert val == "stale_val"