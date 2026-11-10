import argparse
import time
import threading
from concurrent import futures
import grpc

import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, node_id: str, is_leaseholder: bool = False, fault: str = None, delay_ms: int = 0):
        self.node_id = node_id
        self.is_leaseholder = is_leaseholder
        self.fault = fault
        self.delay_ms = delay_ms
        self._lock = threading.Lock()
        
        # Хранилище: key -> (value, timestamp_ms)
        self._values = {}
        # Идемпотентность: idempotency_key -> (key, value, timestamp_ms)
        self._seen = {}
        self.clock = LamportClock()

    def _log_event(self, event_type: str, details: str, l_time: int):
        print(f"[{self.node_id}] {event_type} {details} L={l_time}", flush=True)

    def Put(self, request, context):
        # 1. Симуляция инъекции сбоев
        if self.fault == "drop-after-recv":
            context.abort(grpc.StatusCode.UNAVAILABLE, "Fault injected: drop-after-recv")

        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        # 2. Обновление часов Лампорта при получении
        recv_l = self.clock.update_on_receive(request.lamport_time)
        self._log_event("RECV", f"Put(key={request.key}, value={request.value}, strategy={request.strategy})", recv_l)

        with self._lock:
            # 3. Проверка на дубликат (Idempotency)
            if request.idempotency_key in self._seen:
                k, v, ts = self._seen[request.idempotency_key]
                reply_l = self.clock.increment()
                self._log_event("SEND", f"PutReply(success=True, duplicate=True)", reply_l)
                return kvstore_pb2.PutReply(
                    success=True,
                    acks_received=1,
                    error_message="",
                    lamport_time=reply_l
                )

            # 4. Запись с разрешением конфликтов Last-Write-Wins (LWW)
            curr_val, curr_ts = self._values.get(request.key, ("", 0))
            if request.timestamp >= curr_ts:
                self._values[request.key] = (request.value, request.timestamp)
                self._seen[request.idempotency_key] = (request.key, request.value, request.timestamp)

            apply_l = self.clock.increment()
            self._log_event("APPLY", f"key={request.key} -> {request.value}", apply_l)

            reply_l = self.clock.increment()
            self._log_event("SEND", f"PutReply(success=True, acks=1)", reply_l)

            return kvstore_pb2.PutReply(
                success=True,
                acks_received=1,
                error_message="",
                lamport_time=reply_l
            )

    def Get(self, request, context):
        if self.delay_ms > 0:
            time.sleep(self.delay_ms / 1000.0)

        recv_l = self.clock.update_on_receive(request.lamport_time)
        self._log_event("RECV", f"Get(key={request.key}, consistency={request.consistency})", recv_l)

        # 1. Проверка строгой согласованности (Linearizable)
        # Если узел не является Leaseholder, он отклоняет запрос
        if request.consistency == kvstore_pb2.LINEARIZABLE and not self.is_leaseholder:
            reply_l = self.clock.increment()
            self._log_event("SEND", f"GetReply(error=NOT_LEASEHOLDER)", reply_l)
            return kvstore_pb2.GetReply(
                found=False,
                error_message="NOT_LEASEHOLDER",
                lamport_time=reply_l
            )

        with self._lock:
            found = request.key in self._values
            val, ts = self._values.get(request.key, ("", 0))

            now_ms = int(time.time() * 1000)
            is_stale = False

            # 2. Проверка Bounded Staleness при FOLLOWER_STALE
            if request.consistency == kvstore_pb2.FOLLOWER_STALE:
                if (now_ms - ts) > request.max_staleness_ms and ts != 0:
                    reply_l = self.clock.increment()
                    self._log_event("SEND", f"GetReply(error=STALE_READ_EXCEEDED)", reply_l)
                    return kvstore_pb2.GetReply(
                        found=False,
                        error_message="STALE_READ_EXCEEDED",
                        lamport_time=reply_l
                    )
                is_stale = not self.is_leaseholder

            reply_l = self.clock.increment()
            self._log_event("SEND", f"GetReply(value={val}, found={found}, is_stale={is_stale})", reply_l)

            return kvstore_pb2.GetReply(
                value=val,
                found=found,
                read_timestamp=ts,
                is_stale=is_stale,
                error_message="",
                lamport_time=reply_l
            )

def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--node-id", type=str, default="replica-A")
    parser.add_argument("--leaseholder", action="store_true", default=False)
    parser.add_argument("--fault", type=str, default=None)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(
            node_id=args.node_id, 
            is_leaseholder=args.leaseholder, 
            fault=args.fault, 
            delay_ms=args.delay_ms
        ),
        server
    )
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"Server {args.node_id} (Leaseholder={args.leaseholder}) running on port {args.port}...", flush=True)
    server.wait_for_termination()

if __name__ == "__main__":
    serve()