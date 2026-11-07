#!/usr/bin/env python3
"""Mock external world for the Relay capstone. Zero dependencies (Python 3.9+ stdlib).

Plays the part of every external system Relay workflows act on — email, chat,
orders, refunds, shipments — and records every call it receives in a ledger.
The ledger is the ground truth for exactly-once verification: after a
kill-and-resume drill, scripts/duplication_check.py reads it to prove no side
effect fired twice.

    python3 mock_world.py --port 9210

Side-effect endpoints (all POST, all accept an optional Idempotency-Key header):
    POST /email/send                {"to", "subject"?, "message"}
    POST /chat/message              {"channel", "message"}
    POST /orders/{id}/refund        {"amount_usd"?}   omitted = full refund
    POST /orders/{id}/replacement   {}
    POST /shipments                 {"order_id"}

Idempotency: a repeated Idempotency-Key returns the original response with
header x-mockworld-replayed: true and a ledger entry marked replayed — the
side effect is NOT repeated. Calls without a key always execute.

Read endpoints:
    GET  /health
    GET  /orders/{id}               seeded: ord_2001, ord_2002, ord_2003

Admin:
    GET  /admin/ledger?since=N      entries with seq > N
    POST /admin/reset               clear ledger + idempotency cache, reset orders
    GET  /admin/config
    POST /admin/config              failure injection for non-admin routes, e.g.
                                    {"mode": "down"}          everything -> 503
                                    {"fail_rate": 0.3}        30% of calls -> 500
                                    {"latency_ms": 3000}      delay before responding
                                    {"mode": "ok", "fail_rate": 0, "latency_ms": 0}

Business rules (so sloppy engines get caught):
    - refund on an unknown order -> 404
    - refund amount above the order total -> 400
    - second refund on an already-refunded order (without idempotency replay) -> 409
    - shipment for an unknown order -> 404

Order ord_2001 stays in status "processing" forever — wf_runaway polls it, by design.
"""

import argparse
import json
import random
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEED_ORDERS = {
    "ord_2001": {"order_id": "ord_2001", "status": "processing", "amount_usd": 89.00, "customer_email": "maya@example.com", "item": "Wireless earbuds"},
    "ord_2002": {"order_id": "ord_2002", "status": "delivered", "amount_usd": 45.50, "customer_email": "arjun@example.com", "item": "Bluetooth speaker"},
    "ord_2003": {"order_id": "ord_2003", "status": "processing", "amount_usd": 210.00, "customer_email": "lena@example.com", "item": "Standing desk"},
}

LOCK = threading.Lock()
STATE = {}


def reset_state():
    with LOCK:
        STATE["orders"] = json.loads(json.dumps(SEED_ORDERS))
        STATE["ledger"] = []
        STATE["idempotency"] = {}  # key -> stored response (status, body)
        STATE["seq"] = 0
        STATE["config"] = {"mode": "ok", "fail_rate": 0.0, "latency_ms": 0}


reset_state()


class Handler(BaseHTTPRequestHandler):
    server_version = "MockWorld/1.0"

    def log_message(self, fmt, *args):
        print(f"[world] {self.address_string()} {fmt % args}")

    def _send_json(self, status, obj, headers=None):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message):
        self._send_json(status, {"error": {"message": message}})

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            return None

    def _inject_failure(self):
        """Returns True if an injected failure was sent. Applies to non-admin routes."""
        with LOCK:
            cfg = dict(STATE["config"])
        if cfg["latency_ms"]:
            time.sleep(cfg["latency_ms"] / 1000)
        if cfg["mode"] == "down":
            self._error(503, "Mock world is down (injected)")
            return True
        if cfg["fail_rate"] and random.random() < float(cfg["fail_rate"]):
            self._error(500, "Internal error (injected)")
            return True
        return False

    def _record(self, action, payload, idem_key, replayed, status):
        with LOCK:
            STATE["seq"] += 1
            entry = {
                "seq": STATE["seq"],
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "action": action,
                "payload": payload,
                "idempotency_key": idem_key,
                "replayed": replayed,
                "status": status,
            }
            STATE["ledger"].append(entry)
        return entry

    def _side_effect(self, action, payload, execute):
        """Common idempotency + ledger wrapper. `execute` returns (status, body) and
        runs under LOCK — it must not block."""
        idem_key = self.headers.get("Idempotency-Key")
        if idem_key:
            with LOCK:
                stored = STATE["idempotency"].get(idem_key)
            if stored is not None:
                self._record(action, payload, idem_key, replayed=True, status=stored["status"])
                return self._send_json(stored["status"], stored["body"],
                                       headers={"x-mockworld-replayed": "true"})
        with LOCK:
            status, body = execute()
        self._record(action, payload, idem_key, replayed=False, status=status)
        if idem_key and 200 <= status < 300:
            with LOCK:
                STATE["idempotency"][idem_key] = {"status": status, "body": body}
        self._send_json(status, body)

    def do_GET(self):
        if self.path == "/health":
            return self._send_json(200, {"status": "ok", "service": "mock-world"})
        if self.path == "/admin/config":
            with LOCK:
                return self._send_json(200, dict(STATE["config"]))
        if self.path.startswith("/admin/ledger"):
            since = 0
            if "since=" in self.path:
                try:
                    since = int(self.path.split("since=")[1].split("&")[0])
                except ValueError:
                    return self._error(400, "since must be an integer")
            with LOCK:
                entries = [e for e in STATE["ledger"] if e["seq"] > since]
            return self._send_json(200, {"entries": entries, "count": len(entries)})
        if self._inject_failure():
            return
        if self.path.startswith("/orders/"):
            order_id = self.path.split("/")[2]
            with LOCK:
                order = STATE["orders"].get(order_id)
            if not order:
                return self._error(404, f"Unknown order {order_id}")
            return self._send_json(200, order)
        return self._error(404, f"Unknown path {self.path}")

    def do_POST(self):
        payload = self._read_body()
        if payload is None:
            return self._error(400, "Request body is not valid JSON")

        if self.path == "/admin/reset":
            reset_state()
            return self._send_json(200, {"status": "reset"})
        if self.path == "/admin/config":
            with LOCK:
                for key in ("mode", "fail_rate", "latency_ms"):
                    if key in payload:
                        STATE["config"][key] = payload[key]
                return self._send_json(200, dict(STATE["config"]))

        if self._inject_failure():
            return

        if self.path == "/email/send":
            for field in ("to", "message"):
                if not payload.get(field):
                    return self._error(400, f"Missing field '{field}'")

            def execute():
                return 200, {"delivered": True, "notification_id": f"eml_{uuid.uuid4().hex[:10]}"}
            return self._side_effect("email.send", payload, execute)

        if self.path == "/chat/message":
            for field in ("channel", "message"):
                if not payload.get(field):
                    return self._error(400, f"Missing field '{field}'")

            def execute():
                return 200, {"delivered": True, "notification_id": f"msg_{uuid.uuid4().hex[:10]}"}
            return self._side_effect("chat.message", payload, execute)

        if self.path == "/shipments":
            order_id = payload.get("order_id")
            if not order_id:
                return self._error(400, "Missing field 'order_id'")

            def execute():
                if order_id not in STATE["orders"]:
                    return 404, {"error": {"message": f"Unknown order {order_id}"}}
                return 201, {"shipment_id": f"shp_{uuid.uuid4().hex[:10]}", "order_id": order_id, "status": "created"}
            return self._side_effect("shipment.create", payload, execute)

        if self.path.startswith("/orders/") and self.path.endswith("/refund"):
            order_id = self.path.split("/")[2]

            def execute():
                order = STATE["orders"].get(order_id)
                if not order:
                    return 404, {"error": {"message": f"Unknown order {order_id}"}}
                if order["status"] == "refunded":
                    return 409, {"error": {"message": f"Order {order_id} is already refunded"}}
                amount = payload.get("amount_usd", order["amount_usd"])
                if not isinstance(amount, (int, float)) or amount <= 0:
                    return 400, {"error": {"message": "amount_usd must be a positive number"}}
                if amount > order["amount_usd"]:
                    return 400, {"error": {"message": f"Refund of ${amount} exceeds order total ${order['amount_usd']}"}}
                order["status"] = "refunded"
                return 200, {"status": "refunded", "reference_id": f"ref_{uuid.uuid4().hex[:10]}", "amount_usd": amount}
            return self._side_effect("order.refund", {"order_id": order_id, **payload}, execute)

        if self.path.startswith("/orders/") and self.path.endswith("/replacement"):
            order_id = self.path.split("/")[2]

            def execute():
                if order_id not in STATE["orders"]:
                    return 404, {"error": {"message": f"Unknown order {order_id}"}}
                return 201, {"status": "replacement_created", "reference_id": f"rpl_{uuid.uuid4().hex[:10]}", "order_id": order_id}
            return self._side_effect("order.replacement", {"order_id": order_id}, execute)

        return self._error(404, f"Unknown path {self.path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=9210)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Mock world on http://localhost:{args.port}")
    print("  side effects: /email/send /chat/message /shipments /orders/{id}/refund /orders/{id}/replacement")
    print("  ledger: GET /admin/ledger   reset: POST /admin/reset   failure injection: POST /admin/config")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
