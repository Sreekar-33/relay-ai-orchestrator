"""Worker: claim a job (SKIP LOCKED + lease), heartbeat while processing, run engine ticks. Restart-safe."""
from __future__ import annotations

import logging
import signal
import threading

from relay.config import Settings
from relay.db import Database, migrate
from relay.engine import Engine, Faults, jobs_claim
from relay.validation import load_catalog

log = logging.getLogger("relay.worker")


class Worker:
    def __init__(self, db, engine, settings):
        self.db, self.engine, self.settings = db, engine, settings
        self.worker_id = settings.worker_id
        self._stop, self._wake = threading.Event(), threading.Event()

    def stop(self):
        self._stop.set()
        self._wake.set()

    def reconcile(self) -> int:
        """Defence in depth: every queued/running run must have a job row (e.g. after a restart)."""
        with self.db.tx() as cur:
            cur.execute("""INSERT INTO jobs(run_id) SELECT r.id FROM runs r WHERE r.status IN ('queued','running')
                           AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.run_id=r.id) ON CONFLICT DO NOTHING""")
            return cur.rowcount

    def run_once(self) -> bool:
        with self.db.tx() as cur:
            run_id = jobs_claim(cur, self.worker_id, self.settings.lease_seconds)
        if run_id is None:
            return False
        stop_hb = threading.Event()

        def heartbeat():
            while not stop_hb.wait(max(self.settings.lease_seconds / 3, 0.5)):
                try:
                    with self.db.tx() as cur:
                        cur.execute("UPDATE jobs SET locked_until=now()+make_interval(secs=>%s) WHERE run_id=%s AND locked_by=%s",
                                    (self.settings.lease_seconds, run_id, self.worker_id))
                except Exception:
                    log.exception("heartbeat failed")

        hb = threading.Thread(target=heartbeat, daemon=True)
        hb.start()
        try:
            self.engine.process(run_id, self.worker_id)
        finally:
            stop_hb.set()
            hb.join(timeout=2)
        return True

    def _listen(self):
        while not self._stop.is_set():
            try:
                with self.db.connect_raw() as conn:
                    conn.execute("LISTEN relay_jobs")
                    while not self._stop.is_set():
                        for _ in conn.notifies(timeout=1.0, stop_after=1):
                            self._wake.set()
            except Exception:
                log.warning("LISTEN dropped; retrying")
                self._stop.wait(1.0)

    def run_forever(self):
        log.info("worker %s starting; reconciled %d run(s)", self.worker_id, self.reconcile())
        threading.Thread(target=self._listen, daemon=True).start()
        while not self._stop.is_set():
            try:
                worked = self.run_once()
            except Exception:
                log.exception("worker iteration failed")
                worked = False
            if not worked:
                self._wake.wait(self.settings.poll_interval_s)
                self._wake.clear()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    db = Database(settings.database_url, max_size=5)
    migrate(db)
    faults = Faults(db, settings.faults) if settings.faults else None
    worker = Worker(db, Engine(db, load_catalog(settings.data_dir / "node_catalog.json"), settings, faults=faults), settings)
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    worker.run_forever()


if __name__ == "__main__":
    main()
