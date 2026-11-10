import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import grpc

import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

class ReplicatedKVClient:
    def __init__(self, replica_addresses, leaseholder_addr="localhost:50051", timeout=2.0):
        self.replica_addresses = replica_addresses
        self.leaseholder_addr = leaseholder_addr
        self.timeout = timeout
        self.clock = LamportClock()

    def _send_put(self, addr, request):
        try:
            channel = grpc.insecure_channel(addr)
            stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
            reply = stub.Put(request, timeout=self.timeout)
            channel.close()
            return True, reply
        except grpc.RpcError:
            return False, None

    def put(self, key: str, value: str, strategy=kvstore_pb2.QUORUM_CP, idempotency_key=None):
        if not idempotency_key:
            idempotency_key = str(uuid.uuid4())
        
        send_l = self.clock.increment()
        ts = int(time.time() * 1000)
        
        req = kvstore_pb2.PutRequest(
            key=key,
            value=value,
            idempotency_key=idempotency_key,
            strategy=strategy,
            timestamp=ts,
            lamport_time=send_l
        )

        results = []
        with ThreadPoolExecutor(max_workers=len(self.replica_addresses)) as executor:
            futures_map = {
                executor.submit(self._send_put, addr, req): addr 
                for addr in self.replica_addresses
            }
            for fut in as_completed(futures_map):
                success, reply = fut.result()
                if success and reply.success:
                    results.append(reply)

        acks = len(results)
        majority_threshold = len(self.replica_addresses) // 2 + 1

        if strategy == kvstore_pb2.QUORUM_CP:
            if acks >= majority_threshold:
                return True, acks, ""
            else:
                return False, acks, "Quorum loss / ReplicaUnavailableError"
        else: # ALWAYS_AP
            if acks >= 1:
                return True, acks, ""
            else:
                return False, 0, "All nodes unavailable"

    def get(self, key: str, consistency=kvstore_pb2.LINEARIZABLE, max_staleness_ms=5000, target_addr=None):
        send_l = self.clock.increment()
        req = kvstore_pb2.GetRequest(
            key=key,
            consistency=consistency,
            max_staleness_ms=max_staleness_ms,
            lamport_time=send_l
        )

        target = target_addr if target_addr else (
            self.leaseholder_addr if consistency == kvstore_pb2.LINEARIZABLE else self.replica_addresses[0]
        )

        try:
            channel = grpc.insecure_channel(target)
            stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
            reply = stub.Get(req, timeout=self.timeout)
            channel.close()
            
            if reply.error_message:
                return False, "", reply.error_message
            return reply.found, reply.value, ""
        except grpc.RpcError as e:
            return False, "", str(e)